package main

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Most session-state changes are detected from protocol signals (CommandComplete tags, ParameterStatus).
// Text rules remain only for what the protocol does not reveal: set_config() and temp objects.
var taintRe = regexp.MustCompile(`(?i)set_config|pg_temp|\btemp(orary)?\b`)

// SQL-level access to protocol-level prepared statements: make locally-held statements exist on the server first.
var stmtRefRe = regexp.MustCompile(`(?i)\b(execute|deallocate)\b|pg_prepared_statements`)

var taintTags = [][]byte{[]byte("SET"), []byte("RESET"), []byte("DISCARD"), []byte("DEALLOCATE"), []byte("PREPARE"), []byte("LISTEN"), []byte("UNLISTEN"), []byte("LOAD")}

type stmt struct {
	query     string
	oids      []uint32
	t         *tmpl
	onBackend bool
}

type portal struct {
	st    *stmt
	pfmt  int16
	param []byte
	rfmts []int16
}

type expItem struct {
	kind      byte // 'p' parse, 'c' close
	stmtClose bool // 'c' of a prepared statement sent by the client (not a portal, not injected)
	inject    bool
	name      string
	st        *stmt
	seg       int64
}

type session struct {
	bc           *bconn
	mu           sync.Mutex
	pendingSyncs int
	openSeg      bool
	sendSeg      int64
	doneSeg      int64
	exp          []expItem
	dead         bool
	verifyCh     chan string // non-nil while a state verification is in flight
	vhash        string
	vrow         [][]byte
	swallow      []swallowSeg // replayed segments whose responses were already sent to the client
}

type swallowSeg struct {
	expect  [][]byte
	rowSeen bool
}

type client struct {
	forceNeutral bool // set only on the client goroutine around txc replays
	conn         net.Conn
	be           *pgproto3.Backend
	wmu          sync.Mutex
	eng          *engine
	startup      map[string]string
	mu           sync.Mutex // protects stmts, txStatus, tainted, state
	stmts        map[string]*stmt
	portals      map[string]*portal
	sess         *session
	tainted      bool // session state may differ from the engine baseline: verify before any local answer
	stateBad     bool // last verification found a different session state
	txStatus     byte
	pendingClose []string

	creds     *creds
	keyPID    uint32
	keySecret []byte
	cancelCh  chan struct{}
	sessPtr   atomic.Pointer[session]
	quiet     atomic.Bool        // blocked reading the next client message with no work outstanding
	txc       txState            // transaction-template coalescing state
	vnames    []string           // watched placeholder names at last verification
	watch     map[string]*string // their values in this client's session (nil map: pristine session, all unset)
}

// ---------- client registry (cancel keys, graceful shutdown) ----------

var (
	regMu        sync.Mutex
	clientsByPID = map[uint32]*client{}
	shuttingDown atomic.Bool
	tlsServerCfg *tls.Config
)

func register(c *client) {
	regMu.Lock()
	defer regMu.Unlock()
	for {
		var b [8]byte
		rand.Read(b[:])
		pid := binary.BigEndian.Uint32(b[:4]) & 0x7fffffff
		if pid == 0 || clientsByPID[pid] != nil {
			continue
		}
		c.keyPID, c.keySecret = pid, append([]byte(nil), b[4:]...)
		clientsByPID[pid] = c
		return
	}
}

func unregister(c *client) {
	regMu.Lock()
	delete(clientsByPID, c.keyPID)
	regMu.Unlock()
}

func handleCancel(x *pgproto3.CancelRequest) {
	stCancels.Add(1)
	regMu.Lock()
	c := clientsByPID[x.ProcessID]
	regMu.Unlock()
	if c == nil || subtle.ConstantTimeCompare(c.keySecret, x.SecretKey) != 1 {
		return
	}
	select {
	case c.cancelCh <- struct{}{}:
	default:
	}
	if s := c.sessPtr.Load(); s != nil {
		s.mu.Lock()
		busy := s.pendingSyncs > 0 || s.openSeg
		pid, sec := s.bc.pid, s.bc.secret
		s.mu.Unlock()
		if busy {
			sendCancel(cfg.UpstreamAddr, pid, sec)
		}
	}
}

func (c *client) drainCancel() {
	select {
	case <-c.cancelCh:
	default:
	}
}

// quiescent: idle in protocol and transaction terms; safe to disconnect on shutdown
func (c *client) quiescent() bool {
	if !c.quiet.Load() || c.txc.active {
		return false
	}
	c.mu.Lock()
	ts := c.txStatus
	c.mu.Unlock()
	if ts != 'I' {
		return false
	}
	if s := c.sessPtr.Load(); s != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pendingSyncs == 0 && !s.openSeg
	}
	return true
}

func (c *client) kick() {
	c.wmu.Lock()
	c.be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"})
	c.be.Flush()
	c.wmu.Unlock()
	c.conn.Close()
}

func fatal(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: msg})
	be.Flush()
}

func copyFrontendMsg(m pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch x := m.(type) {
	case *pgproto3.Parse:
		return &pgproto3.Parse{Name: x.Name, Query: x.Query, ParameterOIDs: append([]uint32(nil), x.ParameterOIDs...)}
	case *pgproto3.Bind:
		ps := make([][]byte, len(x.Parameters))
		for i, p := range x.Parameters {
			if p != nil {
				ps[i] = append(make([]byte, 0, len(p)), p...)
			}
		}
		return &pgproto3.Bind{DestinationPortal: x.DestinationPortal, PreparedStatement: x.PreparedStatement,
			ParameterFormatCodes: append([]int16(nil), x.ParameterFormatCodes...), Parameters: ps, ResultFormatCodes: append([]int16(nil), x.ResultFormatCodes...)}
	case *pgproto3.Describe:
		c := *x
		return &c
	case *pgproto3.Execute:
		c := *x
		return &c
	case *pgproto3.Close:
		c := *x
		return &c
	case *pgproto3.Query:
		return &pgproto3.Query{String: x.String}
	case *pgproto3.CopyData:
		return &pgproto3.CopyData{Data: append([]byte(nil), x.Data...)}
	case *pgproto3.CopyDone:
		return &pgproto3.CopyDone{}
	case *pgproto3.CopyFail:
		return &pgproto3.CopyFail{Message: x.Message}
	case *pgproto3.FunctionCall:
		c := *x
		c.ArgFormatCodes = append([]uint16(nil), x.ArgFormatCodes...)
		args := make([][]byte, len(x.Arguments))
		for i, a := range x.Arguments {
			if a != nil {
				args[i] = append([]byte{}, a...)
			}
		}
		c.Arguments = args
		return &c
	}
	return m
}

// Message size limits, as in Postgres: small until the client is authenticated (PG_MAX_AUTH_TOKEN_LENGTH),
// then the regular protocol maximum (PQ_LARGE_MESSAGE_LIMIT).
const (
	preAuthMaxBody = 65535
	maxClientBody  = 0x3fffffff - 1
)

// recovered: an internal error (bug) on one client connection must cost that connection, not every
// client of the proxy. Deferred first in the goroutines that serve a client.
func recovered(what string) {
	if r := recover(); r != nil {
		stPanics.Add(1)
		log.Printf("internal error in %s; this client connection is closed, the proxy keeps running: %v\n%s", what, r, debug.Stack())
	}
}

func handleClient(conn net.Conn) {
	defer func() { conn.Close() }()
	defer recovered("client handler")
	conn.SetDeadline(time.Now().Add(30 * time.Second)) // startup + auth must finish in time
	be := pgproto3.NewBackend(conn, conn)
	be.SetMaxBodyLen(preAuthMaxBody)
	var sm *pgproto3.StartupMessage
	isTLS := false
	for sm == nil {
		m, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch x := m.(type) {
		case *pgproto3.SSLRequest:
			if tlsServerCfg != nil && !isTLS {
				conn.Write([]byte("S"))
				tc := tls.Server(conn, tlsServerCfg)
				if tc.Handshake() != nil {
					return
				}
				conn = tc
				be = pgproto3.NewBackend(conn, conn)
				be.SetMaxBodyLen(preAuthMaxBody)
				isTLS = true
			} else {
				conn.Write([]byte("N"))
			}
		case *pgproto3.GSSEncRequest:
			conn.Write([]byte("N"))
		case *pgproto3.CancelRequest:
			handleCancel(x)
			return
		case *pgproto3.StartupMessage:
			sm = x
		default:
			return
		}
	}
	if cfg.ClientTLSMode == "require" && !isTLS {
		fatal(be, "28000", "TLS connection required")
		return
	}
	if shuttingDown.Load() {
		fatal(be, "57P03", "the proxy is shutting down")
		return
	}
	params := map[string]string{}
	for k, v := range sm.Parameters {
		params[k] = v
	}
	if params["database"] == "" {
		params["database"] = params["user"]
	}
	if params["replication"] != "" && params["replication"] != "false" && params["replication"] != "off" {
		fatal(be, "08004", "replication connections are not supported by the proxy")
		return
	}
	cr, err := authenticateClient(be, params["user"])
	if err != nil {
		stAuthFail.Add(1)
		fatal(be, "28P01", "password authentication failed for user \""+params["user"]+"\"")
		return
	}
	if cr.clientKey != nil || cr.password != "" || cr.md5hash != "" {
		credMu.Lock()
		liveCreds[params["user"]] = cr
		credMu.Unlock()
	}
	eng, err := getEngine(params)
	if err != nil {
		log.Printf("engine for %q: %v", params["user"], err)
		fatal(be, "08006", "proxy: cannot connect to server")
		return
	}
	defer eng.release()
	conn.SetDeadline(time.Time{})
	be.SetMaxBodyLen(maxClientBody)
	c := &client{conn: conn, be: be, eng: eng, startup: params, stmts: map[string]*stmt{}, portals: map[string]*portal{}, txStatus: 'I', creds: cr, cancelCh: make(chan struct{}, 1)}
	register(c)
	defer unregister(c)
	stClients.Add(1)
	stClientsTotal.Add(1)
	defer stClients.Add(-1)
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range eng.pstatus {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: c.keyPID, SecretKey: c.keySecret})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}
	defer func() {
		if c.sess != nil {
			c.sess.bc.close()
		}
	}()
	var group []pgproto3.FrontendMessage
	for {
		if len(group) == 0 {
			if shuttingDown.Load() && c.quiescent0() {
				c.kick()
				return
			}
			c.quiet.Store(true)
		}
		m, err := be.Receive()
		c.quiet.Store(false)
		if err != nil {
			return
		}
		if traceOn {
			traceMsg(c.keyPID, m)
		}
		if c.sess != nil && c.sessDead() {
			return
		}
		switch x := m.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe, *pgproto3.Execute, *pgproto3.Close:
			group = append(group, copyFrontendMsg(m))
		case *pgproto3.Sync:
			if !c.processGroup(group, true) {
				return
			}
			group = nil
		case *pgproto3.Flush:
			if !c.processGroup(group, false) {
				return
			}
			group = nil
		case *pgproto3.Query:
			if c.txc.active && !c.txcFallback(nil) {
				return
			}
			if len(group) > 0 {
				if !c.forward(group, nil) {
					return
				}
				group = nil
			}
			if !c.handleQuery(x.String) {
				return
			}
		default:
			if c.txc.active && !c.txcFallback(nil) {
				return
			}
			if len(group) > 0 {
				if !c.forward(group, nil) {
					return
				}
				group = nil
			}
			if !c.forwardRaw(copyFrontendMsg(m)) {
				return
			}
		}
	}
}

// quiescent check from the client's own goroutine (quiet flag not yet set)
func (c *client) quiescent0() bool {
	c.quiet.Store(true)
	ok := c.quiescent()
	c.quiet.Store(false)
	return ok
}

// stateOK: fail-closed isolation gate. After anything was forwarded on this client's own session, the
// session's full result-affecting state is fingerprinted (stateQuery) and must equal the coalescer
// connections' fingerprint before this client may be answered from a shared batch again.
func (c *client) stateOK() bool {
	c.mu.Lock()
	t, bad := c.tainted, c.stateBad
	c.mu.Unlock()
	if !t {
		return !bad
	}
	h, err := c.verify()
	ok := err == nil && h != ""
	var row [][]byte
	if ok {
		c.sess.mu.Lock()
		row = c.sess.vrow
		c.sess.mu.Unlock()
		ok = len(row) == 2+len(c.vnames)
	}
	if ok {
		w := map[string]*string{}
		for i, n := range c.vnames {
			if v := row[2+i]; v != nil {
				sv := string(v)
				w[n] = &sv
			} else {
				w[n] = nil
			}
		}
		c.watch = w
		if h != c.eng.fp {
			ok = c.switchEngine(h, row[1])
		}
	}
	c.mu.Lock()
	c.tainted = false
	c.stateBad = !ok
	c.mu.Unlock()
	stVerifies.Add(1)
	if !ok {
		stVerifyMismatch.Add(1)
	}
	return ok
}

// switchEngine moves the client to an engine whose connections reproduce its session-level settings
// (e.g. PostgREST's "SET client_min_messages TO WARNING"), proven by an identical state fingerprint.
func (c *client) switchEngine(hash string, setsJSON []byte) bool {
	var pairs [][2]string // empty: back to the base engine (e.g. after RESET)
	if setsJSON != nil {
		if err := json.Unmarshal(setsJSON, &pairs); err != nil || len(pairs) > 64 {
			return false
		}
	}
	v, err := getEngineSets(c.startup, pairs)
	if err != nil {
		return false
	}
	if v.fp != hash {
		v.release()
		return false
	}
	if v == c.eng {
		v.release()
		return true
	}
	old := c.eng
	c.eng = v
	old.release()
	c.mu.Lock()
	var sts []*stmt
	for _, st := range c.stmts {
		sts = append(sts, st)
	}
	c.mu.Unlock()
	for _, st := range sts {
		t := v.classify(st.query, st.oids)
		c.mu.Lock()
		st.t = t
		c.mu.Unlock()
	}
	stEngineSwitches.Add(1)
	return true
}

func (c *client) verify() (string, error) {
	s := c.sess
	if s == nil {
		return "", fmt.Errorf("no session")
	}
	ch := make(chan string, 1)
	s.mu.Lock()
	if s.dead || s.pendingSyncs != 0 || s.openSeg {
		s.mu.Unlock()
		return "", fmt.Errorf("session busy")
	}
	s.verifyCh = ch
	s.vhash = ""
	s.vrow = nil
	s.pendingSyncs++
	s.sendSeg++
	s.mu.Unlock()
	names, _ := c.eng.watched()
	c.vnames = names
	s.bc.fe.Send(&pgproto3.Query{String: verifyQuery(names)})
	if err := s.bc.fe.Flush(); err != nil {
		return "", err
	}
	select {
	case h, ok := <-ch:
		if !ok {
			return "", fmt.Errorf("session closed")
		}
		return h, nil
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("verify timeout")
	}
}

func (c *client) sessDead() bool {
	c.sess.mu.Lock()
	defer c.sess.mu.Unlock()
	return c.sess.dead
}

func (c *client) canLocal() bool {
	c.mu.Lock()
	ok := c.txStatus == 'I' && (c.tainted || !c.stateBad)
	c.mu.Unlock()
	if !ok {
		return false
	}
	if c.sess != nil {
		c.sess.mu.Lock()
		defer c.sess.mu.Unlock()
		return c.sess.pendingSyncs == 0 && !c.sess.openSeg && !c.sess.dead
	}
	return true
}

func (c *client) send(msgs ...pgproto3.BackendMessage) {
	for _, m := range msgs {
		c.be.Send(m)
	}
}

// ----- simple query -----
func (c *client) handleQuery(q string) bool {
	if c.canLocal() {
		if sp := matchSQL(q); sp != nil && sp.litKind != 0 {
			t := c.eng.classify(sp.tmplSQL, nil)
			stateGood := false
			if t.ok && litCompatible(sp, t.paramOID) && c.stateOK() {
				stateGood = true
				t = c.eng.classify(sp.tmplSQL, nil) // the gate may have switched engines
			}
			if stateGood && t.ok && litCompatible(sp, t.paramOID) {
				rf := make([]int16, len(t.fields))
				c.drainCancel()
				res := c.eng.submit(t, 0, []byte(sp.litText), rf, c.cancelCh)
				c.wmu.Lock()
				if res.err != nil {
					c.send(res.err)
				} else {
					c.send(rowDesc(t.fields, rf))
					for _, r := range res.rows {
						c.send(&pgproto3.DataRow{Values: r})
					}
					c.send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(res.rows)))})
				}
				c.send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				err := c.be.Flush()
				c.wmu.Unlock()
				return err == nil
			}
		}
	}
	stForwardQueries.Add(1)
	return c.forward(nil, &pgproto3.Query{String: q})
}

func litCompatible(sp *spec, oid uint32) bool {
	if sp.litKind == 's' {
		// a backslash means different things depending on standard_conforming_strings; the matcher
		// only understands '' quoting, so such literals are left to Postgres (passthrough)
		return !strings.Contains(sp.litText, "\\")
	}
	switch oid {
	case 21:
		return sp.litInt >= -32768 && sp.litInt <= 32767
	case 23:
		return sp.litInt >= -2147483648 && sp.litInt <= 2147483647
	case 20:
		return true
	}
	return false
}

func rowDesc(fields []pgproto3.FieldDescription, rf []int16) *pgproto3.RowDescription {
	out := make([]pgproto3.FieldDescription, len(fields))
	for i, f := range fields {
		out[i] = f
		out[i].Format = 0
		if i < len(rf) {
			out[i].Format = rf[i]
		}
	}
	return &pgproto3.RowDescription{Fields: out}
}

// ----- extended protocol -----
func (c *client) processGroup(group []pgproto3.FrontendMessage, sync bool) bool {
	if c.txc.active && (!sync || len(group) == 0) {
		if !c.txcFallback(nil) {
			return false
		}
	}
	if sync && len(group) > 0 {
		if handled, ok := c.tryTxc(group); handled {
			return ok
		}
	}
	if len(group) == 0 {
		if c.sess != nil {
			c.sess.mu.Lock()
			busy := c.sess.openSeg || c.sess.pendingSyncs > 0
			c.sess.mu.Unlock()
			if busy {
				return c.forward(nil, nil, sync)
			}
		}
		c.wmu.Lock()
		defer c.wmu.Unlock()
		if sync {
			c.portals = map[string]*portal{}
			c.mu.Lock()
			ts := c.txStatus
			c.mu.Unlock()
			c.send(&pgproto3.ReadyForQuery{TxStatus: ts})
		}
		return c.be.Flush() == nil
	}
	// stateOK may move the client to another engine: eligibility is re-checked against it
	if c.canLocal() && c.localEligible(group) && c.stateOK() && c.localEligible(group) {
		return c.runLocal(group, sync)
	}
	return c.forward(group, nil, sync)
}

func (c *client) localEligible(group []pgproto3.FrontendMessage) bool {
	sim := map[string]*stmt{}
	look := func(n string) *stmt {
		if s, ok := sim[n]; ok {
			return s
		}
		c.mu.Lock()
		s := c.stmts[n]
		c.mu.Unlock()
		if s != nil && s.t != nil {
			if t := c.eng.fresh(s.t); t != s.t {
				c.mu.Lock()
				s.t = t
				c.mu.Unlock()
			}
		}
		return s
	}
	ports := map[string]*stmt{}
	for n, p := range c.portals {
		ports[n] = p.st
	}
	for _, m := range group {
		switch x := m.(type) {
		case *pgproto3.Parse:
			if x.Name != "" && look(x.Name) != nil {
				return false
			}
			t := c.eng.classify(x.Query, x.ParameterOIDs)
			if !t.ok {
				return false
			}
			sim[x.Name] = &stmt{query: x.Query, oids: x.ParameterOIDs, t: t}
		case *pgproto3.Bind:
			s := look(x.PreparedStatement)
			if s == nil || s.t == nil || !s.t.ok || len(x.Parameters) != 1 {
				return false
			}
			if len(x.ParameterFormatCodes) > 1 || (len(x.ParameterFormatCodes) == 1 && x.ParameterFormatCodes[0] != 0 && x.ParameterFormatCodes[0] != 1) {
				return false
			}
			n := len(s.t.fields)
			if !(len(x.ResultFormatCodes) == 0 || len(x.ResultFormatCodes) == 1 || len(x.ResultFormatCodes) == n) {
				return false
			}
			for _, f := range x.ResultFormatCodes {
				if f != 0 && f != 1 {
					return false
				}
			}
			ports[x.DestinationPortal] = s
		case *pgproto3.Describe:
			if x.ObjectType == 'S' {
				s := look(x.Name)
				if s == nil || s.t == nil || !s.t.ok {
					return false
				}
			} else if ports[x.Name] == nil {
				return false
			}
		case *pgproto3.Execute:
			if ports[x.Portal] == nil {
				return false
			}
		case *pgproto3.Close:
			// later messages of this group must see the object as gone, exactly as runLocal will
			if x.ObjectType == 'S' {
				sim[x.Name] = nil
			} else {
				delete(ports, x.Name)
			}
		default:
			return false
		}
	}
	return true
}

func (c *client) runLocal(group []pgproto3.FrontendMessage, sync bool) bool {
	stLocalGroups.Add(1)
	var out []pgproto3.BackendMessage
	errored := false
	for _, m := range group {
		if errored {
			break
		}
		switch x := m.(type) {
		case *pgproto3.Parse:
			t := c.eng.classify(x.Query, x.ParameterOIDs)
			c.mu.Lock()
			c.stmts[x.Name] = &stmt{query: x.Query, oids: x.ParameterOIDs, t: t}
			c.mu.Unlock()
			out = append(out, &pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			c.mu.Lock()
			s := c.stmts[x.PreparedStatement]
			c.mu.Unlock()
			if s == nil || s.t == nil || len(x.Parameters) != 1 {
				return c.localInconsistent("Bind")
			}
			var pf int16
			if len(x.ParameterFormatCodes) == 1 {
				pf = x.ParameterFormatCodes[0]
			}
			c.portals[x.DestinationPortal] = &portal{st: s, pfmt: pf, param: x.Parameters[0], rfmts: normFormats(x.ResultFormatCodes, len(s.t.fields))}
			out = append(out, &pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if x.ObjectType == 'S' {
				c.mu.Lock()
				s := c.stmts[x.Name]
				c.mu.Unlock()
				if s == nil || s.t == nil {
					return c.localInconsistent("Describe statement")
				}
				out = append(out, &pgproto3.ParameterDescription{ParameterOIDs: []uint32{s.t.paramOID}}, rowDesc(s.t.fields, nil))
			} else {
				p := c.portals[x.Name]
				if p == nil || p.st == nil || p.st.t == nil {
					return c.localInconsistent("Describe portal")
				}
				out = append(out, rowDesc(p.st.t.fields, p.rfmts))
			}
		case *pgproto3.Execute:
			p := c.portals[x.Portal]
			if p == nil || p.st == nil || p.st.t == nil {
				return c.localInconsistent("Execute")
			}
			c.drainCancel()
			res := c.eng.submit(p.st.t, p.pfmt, p.param, p.rfmts, c.cancelCh)
			if res.err != nil {
				out = append(out, res.err)
				errored = true
				continue
			}
			for _, r := range res.rows {
				out = append(out, &pgproto3.DataRow{Values: r})
			}
			out = append(out, &pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(res.rows)))})
		case *pgproto3.Close:
			if x.ObjectType == 'S' {
				c.mu.Lock()
				if s := c.stmts[x.Name]; s != nil {
					if s.onBackend && x.Name != "" {
						c.pendingClose = append(c.pendingClose, x.Name)
					}
					delete(c.stmts, x.Name)
				}
				c.mu.Unlock()
			} else {
				delete(c.portals, x.Name)
			}
			out = append(out, &pgproto3.CloseComplete{})
		}
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.send(out...)
	if sync {
		c.portals = map[string]*portal{}
		c.send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
	return c.be.Flush() == nil
}

// localInconsistent: runLocal met a statement or portal that localEligible had accepted but that does
// not exist. That is a proxy bug; fail closed by ending this one connection rather than guessing.
func (c *client) localInconsistent(what string) bool {
	stPanics.Add(1)
	log.Printf("internal error: local %s without its statement/portal (client %d); closing this connection", what, c.keyPID)
	c.wmu.Lock()
	c.be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "XX000", Message: "proxy: internal protocol state error; connection closed"})
	c.be.Flush()
	c.wmu.Unlock()
	return false
}

func (c *client) ensureSession() bool {
	if c.sess != nil {
		return true
	}
	bc, err := dialServer(cfg.UpstreamAddr, c.startup, c.creds)
	if err != nil {
		log.Printf("upstream for %q: %v", c.startup["user"], err)
		c.wmu.Lock()
		c.send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "08006", Message: "proxy: cannot connect to upstream"})
		c.be.Flush()
		c.wmu.Unlock()
		return false
	}
	c.sess = &session{bc: bc}
	c.sessPtr.Store(c.sess)
	go c.relay(c.sess)
	return true
}

func (c *client) checkTaint(q string) {
	if taintRe.MatchString(q) {
		c.mu.Lock()
		c.tainted = true
		c.mu.Unlock()
	}
}

// forward: group messages (+ optional simple query), terminated by Sync (sync=true) or Flush (sync=false) for extended groups.
func (c *client) forward(group []pgproto3.FrontendMessage, q *pgproto3.Query, syncOpt ...bool) bool {
	if !c.ensureSession() {
		return false
	}
	s := c.sess
	fe := s.bc.fe
	stForwardGroups.Add(1)
	neutral := c.forceNeutral || c.neutral(group, q)
	c.mu.Lock()
	if !neutral {
		c.tainted = true
	}
	s.mu.Lock()
	seg := s.sendSeg
	// pending closes of statements closed locally but present on backend
	for _, n := range c.pendingClose {
		fe.Send(&pgproto3.Close{ObjectType: 'S', Name: n})
		s.exp = append(s.exp, expItem{kind: 'c', inject: true, name: n, seg: seg})
	}
	c.pendingClose = nil
	parsedHere := map[string]bool{}
	for _, m := range group {
		switch x := m.(type) {
		case *pgproto3.Parse:
			c.checkTaintLocked(x.Query)
			parsedHere[x.Name] = true
			st := &stmt{query: x.Query, oids: x.ParameterOIDs, onBackend: true}
			st.t = c.eng.classify(x.Query, x.ParameterOIDs)
			fe.Send(x)
			s.exp = append(s.exp, expItem{kind: 'p', name: x.Name, st: st, seg: seg})
		case *pgproto3.Bind:
			c.injectIfNeeded(s, x.PreparedStatement, parsedHere, seg)
			fe.Send(x)
		case *pgproto3.Describe:
			if x.ObjectType == 'S' {
				c.injectIfNeeded(s, x.Name, parsedHere, seg)
			}
			fe.Send(x)
		case *pgproto3.Close:
			if x.ObjectType == 'S' {
				delete(c.stmts, x.Name)
			}
			fe.Send(x)
			s.exp = append(s.exp, expItem{kind: 'c', stmtClose: x.ObjectType == 'S', name: x.Name, seg: seg})
		default:
			fe.Send(m)
		}
	}
	if q != nil {
		c.checkTaintLocked(q.String)
		if stmtRefRe.MatchString(q.String) {
			for n := range c.stmts {
				if n != "" {
					c.injectIfNeeded(s, n, parsedHere, seg)
				}
			}
		}
		fe.Send(q)
		s.pendingSyncs++
		s.sendSeg++
	} else if len(syncOpt) > 0 {
		if syncOpt[0] {
			fe.Send(&pgproto3.Sync{})
			s.pendingSyncs++
			s.openSeg = false
			s.sendSeg++
			c.portals = map[string]*portal{}
		} else {
			fe.Send(&pgproto3.Flush{})
			s.openSeg = true
		}
	} else {
		s.openSeg = true
	}
	s.mu.Unlock()
	c.mu.Unlock()
	if err := fe.Flush(); err != nil {
		return false
	}
	return true
}

// neutral: every statement in the group is a strict-shape read of a plain table whose template passed
// all catalog checks (no RLS, no functions, built-in types; SELECT fires no triggers). Such statements
// cannot change session state, so forwarding them does not require a new state verification.
func (c *client) neutralSQL(sql string, oids []uint32) bool {
	// transaction control never changes post-transaction session state (SET LOCAL/set_config(..,true) revert)
	if txBeginRe.MatchString(sql) || txEndRe.MatchString(sql) {
		return true
	}
	sp := matchSQL(sql)
	if sp == nil {
		return false
	}
	var t *tmpl
	if sp.litKind != 0 {
		t = c.eng.classify(sp.tmplSQL, nil)
	} else {
		t = c.eng.classify(sql, oids)
	}
	return t != nil && t.ok
}

func (c *client) neutral(group []pgproto3.FrontendMessage, q *pgproto3.Query) bool {
	for _, m := range group {
		switch x := m.(type) {
		case *pgproto3.Parse:
			if !c.neutralSQL(x.Query, x.ParameterOIDs) {
				return false
			}
		case *pgproto3.Bind:
			c.mu.Lock()
			st := c.stmts[x.PreparedStatement]
			c.mu.Unlock()
			parsedHere := false
			for _, m2 := range group {
				if p, ok := m2.(*pgproto3.Parse); ok && p.Name == x.PreparedStatement {
					parsedHere = true
				}
			}
			if !parsedHere && (st == nil || !c.neutralSQL(st.query, st.oids)) {
				return false
			}
		case *pgproto3.Describe, *pgproto3.Execute, *pgproto3.Close:
		default:
			return false
		}
	}
	if q != nil && !c.neutralSQL(q.String, nil) {
		return false
	}
	return true
}

func (c *client) checkTaintLocked(q string) {
	if !c.tainted && taintRe.MatchString(q) {
		c.tainted = true
		stTaints.Add(1)
	}
}

// inject a Parse for a statement that only exists locally (caller holds c.mu and s.mu)
func (c *client) injectIfNeeded(s *session, name string, parsedHere map[string]bool, seg int64) {
	if parsedHere[name] {
		return
	}
	// a Parse for this name sent in an earlier, still-unanswered pipelined group (e.g. txc fallback replay)
	// will create it on the backend: injecting again would fail with 42P05
	for i := range s.exp {
		if s.exp[i].kind == 'p' && s.exp[i].name == name {
			return
		}
	}
	st := c.stmts[name]
	if st == nil || st.onBackend {
		return
	}
	s.bc.fe.Send(&pgproto3.Parse{Name: name, Query: st.query, ParameterOIDs: st.oids})
	st.onBackend = true
	s.exp = append(s.exp, expItem{kind: 'p', inject: true, name: name, st: st, seg: seg})
}

func (c *client) forwardRaw(m pgproto3.FrontendMessage) bool {
	if !c.ensureSession() {
		return false
	}
	s := c.sess
	c.mu.Lock()
	c.tainted = true
	c.mu.Unlock()
	s.mu.Lock()
	if _, ok := m.(*pgproto3.FunctionCall); ok {
		s.pendingSyncs++
		s.sendSeg++
	}
	s.mu.Unlock()
	s.bc.fe.Send(m)
	return s.bc.fe.Flush() == nil
}

func (c *client) relay(s *session) {
	defer recovered("session relay")
	defer func() {
		s.mu.Lock()
		s.dead = true
		if s.verifyCh != nil {
			close(s.verifyCh)
			s.verifyCh = nil
		}
		s.mu.Unlock()
		c.conn.Close()
	}()
	unflushed := 0
	for {
		m, err := s.bc.fe.Receive()
		if err != nil {
			return
		}
		s.mu.Lock()
		vch := s.verifyCh
		s.mu.Unlock()
		if vch != nil {
			switch x := m.(type) {
			case *pgproto3.DataRow:
				s.vrow = copyRow(x.Values)
				continue
			case *pgproto3.ReadyForQuery:
				c.mu.Lock() // lock order everywhere: c.mu before s.mu
				s.mu.Lock()
				s.doneSeg++
				if s.pendingSyncs > 0 {
					s.pendingSyncs--
				}
				s.verifyCh = nil
				c.txStatus = x.TxStatus
				s.mu.Unlock()
				c.mu.Unlock()
				if len(s.vrow) > 0 && s.vrow[0] != nil {
					vch <- string(s.vrow[0])
				} else {
					vch <- ""
				}
				continue
			case *pgproto3.ParameterStatus, *pgproto3.NotificationResponse, *pgproto3.NoticeResponse:
				// asynchronous: handled/forwarded normally below
			default:
				continue // RowDescription, CommandComplete, ErrorResponse of the check itself
			}
		}
		s.mu.Lock()
		swallowing := len(s.swallow) > 0
		lie := false
		if swallowing {
			sw := &s.swallow[0]
			switch x := m.(type) {
			case *pgproto3.ErrorResponse:
				lie = true
			case *pgproto3.DataRow:
				if sw.expect != nil {
					if sw.rowSeen || len(x.Values) != len(sw.expect) {
						lie = true
					} else {
						for i := range x.Values {
							if !bytes.Equal(x.Values[i], sw.expect[i]) {
								lie = true
							}
						}
					}
					sw.rowSeen = true
				}
			case *pgproto3.ReadyForQuery:
				if sw.expect != nil && !sw.rowSeen {
					lie = true
				}
				s.swallow = s.swallow[1:]
			}
		}
		s.mu.Unlock()
		if lie {
			// a replay produced something other than what the proxy already answered: never continue
			log.Printf("transaction replay diverged for client %d; closing connection", c.keyPID)
			c.wmu.Lock()
			c.be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "08P01", Message: "proxy: transaction replay diverged; connection closed to stay correct"})
			c.be.Flush()
			c.wmu.Unlock()
			return
		}
		forward := true
		switch x := m.(type) {
		case *pgproto3.CommandComplete:
			tag := x.CommandTag
			for _, t := range taintTags {
				if bytes.HasPrefix(tag, t) && (len(tag) == len(t) || tag[len(t)] == ' ') {
					c.mu.Lock()
					if !c.tainted {
						c.tainted = true
						stTaints.Add(1)
					}
					if bytes.Equal(tag, []byte("DISCARD ALL")) || bytes.Equal(tag, []byte("DEALLOCATE ALL")) {
						c.stmts = map[string]*stmt{}
					}
					c.mu.Unlock()
					break
				}
			}
		case *pgproto3.ParameterStatus:
			if x.Name != "application_name" && s.bc.params[x.Name] != x.Value {
				c.mu.Lock()
				if !c.tainted {
					c.tainted = true
					stTaints.Add(1)
				}
				c.mu.Unlock()
			}
		case *pgproto3.ParseComplete, *pgproto3.CloseComplete:
			c.mu.Lock()
			s.mu.Lock()
			if len(s.exp) > 0 {
				it := s.exp[0]
				s.exp = s.exp[1:]
				forward = !it.inject
				if it.kind == 'p' && !it.inject {
					c.stmts[it.name] = it.st
				}
				if it.kind == 'c' && it.stmtClose {
					// the server closed it now; a Parse of the same name earlier in the same group
					// has just been recorded above and must not outlive this Close
					delete(c.stmts, it.name)
				}
			} else {
				log.Printf("relay: unexpected completion")
			}
			s.mu.Unlock()
			c.mu.Unlock()
		case *pgproto3.ReadyForQuery:
			c.mu.Lock()
			s.mu.Lock()
			s.doneSeg++
			// drop expectations of the finished segment (skipped after an error)
			for len(s.exp) > 0 && s.exp[0].seg < s.doneSeg {
				it := s.exp[0]
				if it.kind == 'p' && it.inject {
					it.st.onBackend = false
				}
				s.exp = s.exp[1:]
			}
			if s.pendingSyncs > 0 {
				s.pendingSyncs--
			}
			c.txStatus = x.TxStatus
			s.mu.Unlock()
			c.mu.Unlock()
		}
		if swallowing {
			switch m.(type) {
			case *pgproto3.ParameterStatus, *pgproto3.NoticeResponse, *pgproto3.NotificationResponse:
			default:
				forward = false
			}
		}
		if forward {
			c.wmu.Lock()
			c.be.Send(m)
			unflushed++
			_, isRow := m.(*pgproto3.DataRow)
			if !isRow || unflushed > 256 {
				if c.be.Flush() != nil {
					c.wmu.Unlock()
					return
				}
				unflushed = 0
			}
			c.wmu.Unlock()
		}
	}
}

var traceOn = os.Getenv("PGC_TRACE") != ""

// debug tracing of client protocol flow (statement text and parameter shapes only, never values)
func traceMsg(pid uint32, m pgproto3.FrontendMessage) {
	switch x := m.(type) {
	case *pgproto3.Parse:
		log.Printf("[%d] Parse %q oids=%v: %s", pid, x.Name, x.ParameterOIDs, x.Query)
	case *pgproto3.Bind:
		log.Printf("[%d] Bind stmt=%q portal=%q nparams=%d pfmt=%v rfmt=%v", pid, x.PreparedStatement, x.DestinationPortal, len(x.Parameters), x.ParameterFormatCodes, x.ResultFormatCodes)
	case *pgproto3.Query:
		log.Printf("[%d] Query: %s", pid, x.String)
	default:
		log.Printf("[%d] %T", pid, m)
	}
}
