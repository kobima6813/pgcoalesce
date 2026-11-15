package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/crypto/pbkdf2"
)

// ---------- credentials ----------

type scramSecret struct {
	iter      int
	salt      []byte
	storedKey []byte
	serverKey []byte
}

type creds struct {
	password  string // plaintext, if known
	md5hash   string // "md5<hex>" if known
	scram     *scramSecret
	clientKey []byte // recovered during client SCRAM auth (enables SCRAM pass-through to the server)
}

func parseScramSecret(s string) *scramSecret {
	// SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>
	if !strings.HasPrefix(s, "SCRAM-SHA-256$") {
		return nil
	}
	p := strings.Split(strings.TrimPrefix(s, "SCRAM-SHA-256$"), "$")
	if len(p) != 2 {
		return nil
	}
	a := strings.Split(p[0], ":")
	b := strings.Split(p[1], ":")
	if len(a) != 2 || len(b) != 2 {
		return nil
	}
	it, err := strconv.Atoi(a[0])
	salt, e1 := base64.StdEncoding.DecodeString(a[1])
	sk, e2 := base64.StdEncoding.DecodeString(b[0])
	svk, e3 := base64.StdEncoding.DecodeString(b[1])
	if err != nil || e1 != nil || e2 != nil || e3 != nil {
		return nil
	}
	return &scramSecret{iter: it, salt: salt, storedKey: sk, serverKey: svk}
}

func hmacSHA(key, msg []byte) []byte { h := hmac.New(sha256.New, key); h.Write(msg); return h.Sum(nil) }
func sha(b []byte) []byte            { s := sha256.Sum256(b); return s[:] }

func scramFromPassword(pw string, salt []byte, iter int) *scramSecret {
	salted := pbkdf2.Key([]byte(pw), salt, iter, 32, sha256.New)
	ck := hmacSHA(salted, []byte("Client Key"))
	return &scramSecret{iter: iter, salt: salt, storedKey: sha(ck), serverKey: hmacSHA(salted, []byte("Server Key"))}
}

func credsFromStored(user, stored string) *creds {
	c := &creds{}
	switch {
	case strings.HasPrefix(stored, "SCRAM-SHA-256$"):
		c.scram = parseScramSecret(stored)
		if c.scram == nil {
			return nil
		}
	case strings.HasPrefix(stored, "md5") && len(stored) == 35:
		c.md5hash = stored
	default:
		c.password = stored
		c.md5hash = "md5" + md5hex(stored+user)
	}
	return c
}

func md5hex(s string) string { h := md5.Sum([]byte(s)); return hex.EncodeToString(h[:]) }

// userlist file, PgBouncer format: "user" "password|md5...|SCRAM-SHA-256$..."
func loadUserlist(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || l[0] == ';' || l[0] == '#' {
			continue
		}
		var fields []string
		for len(l) > 0 {
			l = strings.TrimSpace(l)
			if l == "" || l[0] != '"' {
				break
			}
			v := ""
			i := 1
			for ; i < len(l); i++ {
				if l[i] == '"' {
					if i+1 < len(l) && l[i+1] == '"' {
						v += `"`
						i++
						continue
					}
					break
				}
				v += string(l[i])
			}
			fields = append(fields, v)
			if i+1 > len(l) {
				break
			}
			l = l[i+1:]
		}
		if len(fields) >= 2 {
			out[fields[0]] = fields[1]
		}
	}
	return out, sc.Err()
}

// ---------- credential store ----------

var (
	credMu    sync.Mutex
	userlist  map[string]string
	authCache = map[string]authCacheEntry{}
	liveCreds = map[string]*creds{} // last successful client auth per user (used to dial server connections)
)

type authCacheEntry struct {
	stored string
	ok     bool
	at     time.Time
}

func storedSecret(user string) (string, bool) {
	credMu.Lock()
	if s, ok := userlist[user]; ok {
		credMu.Unlock()
		return s, true
	}
	if e, ok := authCache[user]; ok && time.Since(e.at) < 30*time.Second {
		credMu.Unlock()
		return e.stored, e.ok
	}
	credMu.Unlock()
	if cfg.AuthQuery == "" || cfg.AuthUser == "" {
		return "", false
	}
	s, ok, err := runAuthQuery(user)
	if err != nil {
		return "", false
	}
	credMu.Lock()
	authCache[user] = authCacheEntry{stored: s, ok: ok, at: time.Now()}
	credMu.Unlock()
	return s, ok
}

func runAuthQuery(user string) (string, bool, error) {
	credMu.Lock()
	au, ok := userlist[cfg.AuthUser]
	credMu.Unlock()
	if !ok {
		return "", false, errors.New("auth_user not in auth_file")
	}
	b, err := dialServer(cfg.ServerAddr, map[string]string{"user": cfg.AuthUser, "database": cfg.AuthDB}, credsFromStored(cfg.AuthUser, au))
	if err != nil {
		return "", false, err
	}
	defer b.close()
	b.fe.Send(&pgproto3.Parse{Query: cfg.AuthQuery})
	b.fe.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte(user)}})
	b.fe.Send(&pgproto3.Execute{})
	b.fe.Send(&pgproto3.Sync{})
	if err := b.fe.Flush(); err != nil {
		return "", false, err
	}
	var pw string
	found := false
	var qerr error
	err = b.readUntilRFQ(func(m pgproto3.BackendMessage) {
		switch x := m.(type) {
		case *pgproto3.DataRow:
			if len(x.Values) >= 2 && x.Values[1] != nil {
				pw = string(x.Values[1])
				found = true
			}
		case *pgproto3.ErrorResponse:
			qerr = errors.New(x.Message)
		}
	})
	if err == nil {
		err = qerr
	}
	return pw, found, err
}

// ---------- client-side authentication (proxy acting as server) ----------

var errAuth = errors.New("password authentication failed")

func authenticateClient(be *pgproto3.Backend, user string) (*creds, error) {
	if cfg.AuthType == "trust" {
		credMu.Lock()
		defer credMu.Unlock()
		if s, ok := userlist[user]; ok {
			return credsFromStored(user, s), nil
		}
		return &creds{}, nil
	}
	stored, ok := storedSecret(user)
	var cr *creds
	if ok {
		cr = credsFromStored(user, stored)
	}
	method := cfg.AuthType
	if method == "scram-sha-256" && cr != nil && cr.scram == nil && cr.password == "" {
		method = "md5" // only an md5 hash is stored
	}
	if method == "md5" && cr != nil && cr.md5hash == "" {
		method = "scram-sha-256" // only a SCRAM secret is stored (like Postgres)
	}
	switch method {
	case "md5":
		var salt [4]byte
		rand.Read(salt[:])
		be.Send(&pgproto3.AuthenticationMD5Password{Salt: salt})
		if err := be.Flush(); err != nil {
			return nil, err
		}
		be.SetAuthType(pgproto3.AuthTypeMD5Password)
		m, err := be.Receive()
		if err != nil {
			return nil, err
		}
		pm, ok := m.(*pgproto3.PasswordMessage)
		if !ok || cr == nil || cr.md5hash == "" {
			return nil, errAuth
		}
		want := "md5" + md5hex(cr.md5hash[3:]+string(salt[:]))
		if subtle.ConstantTimeCompare([]byte(want), []byte(pm.Password)) != 1 {
			return nil, errAuth
		}
		return cr, nil
	case "scram-sha-256":
		return scramServer(be, user, cr)
	}
	return nil, fmt.Errorf("unsupported auth_type %q", cfg.AuthType)
}

func scramServer(be *pgproto3.Backend, user string, cr *creds) (*creds, error) {
	be.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
	if err := be.Flush(); err != nil {
		return nil, err
	}
	be.SetAuthType(pgproto3.AuthTypeSASL)
	m, err := be.Receive()
	if err != nil {
		return nil, err
	}
	ir, ok := m.(*pgproto3.SASLInitialResponse)
	if !ok || ir.AuthMechanism != "SCRAM-SHA-256" {
		return nil, errAuth
	}
	cf := string(ir.Data)
	if !(strings.HasPrefix(cf, "n,,") || strings.HasPrefix(cf, "y,,")) {
		return nil, errAuth
	}
	gs2 := cf[:3]
	bare := cf[3:]
	var cnonce string
	for _, kv := range strings.Split(bare, ",") {
		if strings.HasPrefix(kv, "r=") {
			cnonce = kv[2:]
		}
	}
	if cnonce == "" {
		return nil, errAuth
	}
	// unknown user → use a fake secret so the exchange fails the same way (no user enumeration)
	sec := (*scramSecret)(nil)
	if cr != nil {
		if cr.scram != nil {
			sec = cr.scram
		} else if cr.password != "" {
			salt := make([]byte, 16)
			rand.Read(salt)
			sec = scramFromPassword(cr.password, salt, 4096)
		}
	}
	fake := sec == nil
	if fake {
		salt := make([]byte, 16)
		rand.Read(salt)
		sec = &scramSecret{iter: 4096, salt: salt, storedKey: make([]byte, 32), serverKey: make([]byte, 32)}
	}
	sn := make([]byte, 18)
	rand.Read(sn)
	nonce := cnonce + base64.StdEncoding.EncodeToString(sn)
	sf := "r=" + nonce + ",s=" + base64.StdEncoding.EncodeToString(sec.salt) + ",i=" + strconv.Itoa(sec.iter)
	be.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(sf)})
	if err := be.Flush(); err != nil {
		return nil, err
	}
	be.SetAuthType(pgproto3.AuthTypeSASLContinue)
	m, err = be.Receive()
	if err != nil {
		return nil, err
	}
	rsp, ok := m.(*pgproto3.SASLResponse)
	if !ok {
		return nil, errAuth
	}
	fin := string(rsp.Data)
	idx := strings.LastIndex(fin, ",p=")
	if idx < 0 {
		return nil, errAuth
	}
	without := fin[:idx]
	proof, err := base64.StdEncoding.DecodeString(fin[idx+3:])
	if err != nil || len(proof) != 32 {
		return nil, errAuth
	}
	var cb, rn string
	for _, kv := range strings.Split(without, ",") {
		if strings.HasPrefix(kv, "c=") {
			cb = kv[2:]
		}
		if strings.HasPrefix(kv, "r=") {
			rn = kv[2:]
		}
	}
	if cb != base64.StdEncoding.EncodeToString([]byte(gs2)) || rn != nonce {
		return nil, errAuth
	}
	authMsg := bare + "," + sf + "," + without
	csig := hmacSHA(sec.storedKey, []byte(authMsg))
	ck := make([]byte, 32)
	for i := range ck {
		ck[i] = proof[i] ^ csig[i]
	}
	if fake || subtle.ConstantTimeCompare(sha(ck), sec.storedKey) != 1 {
		return nil, errAuth
	}
	ssig := hmacSHA(sec.serverKey, []byte(authMsg))
	be.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte("v=" + base64.StdEncoding.EncodeToString(ssig))})
	out := *cr
	out.scram = sec
	out.clientKey = ck
	return &out, nil
}

// ---------- server connections (proxy acting as client) ----------

type bconn struct {
	conn     net.Conn
	fe       *pgproto3.Frontend
	prepared map[string]bool
	broken   bool
	params   map[string]string
	pid      uint32
	secret   []byte
	addr     string
	born     time.Time
}

func (b *bconn) close() {
	b.fe.Send(&pgproto3.Terminate{})
	b.fe.Flush()
	b.conn.Close()
}

func serverTLS(conn net.Conn, addr string) (net.Conn, error) {
	mode := cfg.ServerTLSMode
	if mode == "" || mode == "disable" {
		return conn, nil
	}
	conn.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47}) // SSLRequest
	var r [1]byte
	if _, err := conn.Read(r[:]); err != nil {
		return nil, err
	}
	if r[0] != 'S' {
		if mode == "prefer" || mode == "allow" {
			return conn, nil
		}
		return nil, errors.New("server does not support TLS")
	}
	tc := &tls.Config{InsecureSkipVerify: true}
	host, _, _ := net.SplitHostPort(addr)
	if mode == "verify-ca" || mode == "verify-full" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.ServerTLSCAFile)
		if err != nil {
			return nil, err
		}
		pool.AppendCertsFromPEM(pem)
		if mode == "verify-full" {
			tc = &tls.Config{RootCAs: pool, ServerName: firstNonEmpty(cfg.ServerTLSServerName, host)}
		} else {
			tc = &tls.Config{InsecureSkipVerify: true, VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
				certs := make([]*x509.Certificate, len(raw))
				for i, r := range raw {
					c, err := x509.ParseCertificate(r)
					if err != nil {
						return err
					}
					certs[i] = c
				}
				inter := x509.NewCertPool()
				for _, c := range certs[1:] {
					inter.AddCert(c)
				}
				_, err := certs[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter})
				return err
			}}
		}
	}
	t := tls.Client(conn, tc)
	if err := t.Handshake(); err != nil {
		return nil, err
	}
	return t, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func dialServer(addr string, startup map[string]string, cr *creds) (*bconn, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	c, err := serverTLS(conn, addr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	fe := pgproto3.NewFrontend(c, c)
	params := map[string]string{}
	for k, v := range startup {
		params[k] = v
	}
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: params})
	if err := fe.Flush(); err != nil {
		c.Close()
		return nil, err
	}
	b := &bconn{conn: c, fe: fe, prepared: map[string]bool{}, params: map[string]string{}, addr: addr, born: time.Now()}
	user := startup["user"]
	var scramState struct {
		bare, nonce string
		sec         *scramSecret
		ck          []byte
		authMsg     string
	}
	fail := func(e error) (*bconn, error) { c.Close(); return nil, e }
	for {
		m, err := fe.Receive()
		if err != nil {
			return fail(err)
		}
		switch x := m.(type) {
		case *pgproto3.AuthenticationOk:
		case *pgproto3.AuthenticationCleartextPassword:
			if cr == nil || cr.password == "" {
				return fail(errors.New("server requested cleartext password; no plaintext password available"))
			}
			fe.Send(&pgproto3.PasswordMessage{Password: cr.password})
			fe.Flush()
		case *pgproto3.AuthenticationMD5Password:
			if cr == nil || cr.md5hash == "" {
				return fail(errors.New("server requested md5; no md5 credential available"))
			}
			fe.Send(&pgproto3.PasswordMessage{Password: "md5" + md5hex(cr.md5hash[3:]+string(x.Salt[:]))})
			fe.Flush()
		case *pgproto3.AuthenticationSASL:
			if cr == nil || (cr.password == "" && cr.clientKey == nil) {
				return fail(errors.New("server requested SCRAM; no password or client key available"))
			}
			nb := make([]byte, 18)
			rand.Read(nb)
			scramState.nonce = base64.StdEncoding.EncodeToString(nb)
			scramState.bare = "n=,r=" + scramState.nonce
			fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte("n,," + scramState.bare)})
			fe.Flush()
		case *pgproto3.AuthenticationSASLContinue:
			sf := string(x.Data)
			var rn, s string
			it := 0
			for _, kv := range strings.Split(sf, ",") {
				switch {
				case strings.HasPrefix(kv, "r="):
					rn = kv[2:]
				case strings.HasPrefix(kv, "s="):
					s = kv[2:]
				case strings.HasPrefix(kv, "i="):
					it, _ = strconv.Atoi(kv[2:])
				}
			}
			salt, err := base64.StdEncoding.DecodeString(s)
			if err != nil || !strings.HasPrefix(rn, scramState.nonce) || it <= 0 {
				return fail(errors.New("bad SCRAM server-first"))
			}
			if cr.password != "" {
				salted := pbkdf2.Key([]byte(cr.password), salt, it, 32, sha256.New)
				scramState.ck = hmacSHA(salted, []byte("Client Key"))
				scramState.sec = &scramSecret{iter: it, salt: salt, storedKey: sha(scramState.ck), serverKey: hmacSHA(salted, []byte("Server Key"))}
			} else {
				if cr.scram == nil || it != cr.scram.iter || !hmac.Equal(salt, cr.scram.salt) {
					return fail(errors.New("SCRAM pass-through: server secret differs from the one used for client auth"))
				}
				scramState.ck = cr.clientKey
				scramState.sec = cr.scram
			}
			without := "c=biws,r=" + rn
			scramState.authMsg = scramState.bare + "," + sf + "," + without
			sig := hmacSHA(scramState.sec.storedKey, []byte(scramState.authMsg))
			proof := make([]byte, 32)
			for i := range proof {
				proof[i] = scramState.ck[i] ^ sig[i]
			}
			fe.Send(&pgproto3.SASLResponse{Data: []byte(without + ",p=" + base64.StdEncoding.EncodeToString(proof))})
			fe.Flush()
		case *pgproto3.AuthenticationSASLFinal:
			want := "v=" + base64.StdEncoding.EncodeToString(hmacSHA(scramState.sec.serverKey, []byte(scramState.authMsg)))
			if string(x.Data) != want {
				return fail(errors.New("SCRAM server signature mismatch"))
			}
		case *pgproto3.ParameterStatus:
			b.params[x.Name] = x.Value
		case *pgproto3.BackendKeyData:
			b.pid = x.ProcessID
			b.secret = append([]byte(nil), x.SecretKey...)
		case *pgproto3.ErrorResponse:
			return fail(fmt.Errorf("server: %s (%s) for user %s", x.Message, x.Code, user))
		case *pgproto3.ReadyForQuery:
			c.SetDeadline(time.Time{})
			return b, nil
		}
	}
}

func sendCancel(addr string, pid uint32, secret []byte) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	c, err := serverTLS(conn, addr)
	if err != nil {
		return
	}
	buf := make([]byte, 12, 12+len(secret))
	binary.BigEndian.PutUint32(buf[0:], uint32(12+len(secret)))
	binary.BigEndian.PutUint32(buf[4:], 80877102)
	binary.BigEndian.PutUint32(buf[8:], pid)
	buf = append(buf, secret...)
	binary.BigEndian.PutUint32(buf[0:], uint32(len(buf)))
	c.Write(buf)
	c.Read(make([]byte, 1)) // server closes
}
