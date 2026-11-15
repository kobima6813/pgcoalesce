// Adversarial isolation test for transaction-template coalescing (PostgREST request shape).
// Each persona runs PostgREST-identical transactions through the proxy and the same transactions on a
// direct Postgres connection with the same session state; every result (rows, errors, command tags)
// must match. Passwords and JWT secret are read from 600 files and never printed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const readProfiles = `WITH pgrst_source AS ( SELECT "api"."profiles".* FROM "api"."profiles" WHERE  "api"."profiles"."id" = $1    )  SELECT null::bigint AS total_result_set, pg_catalog.count(_postgrest_t) AS page_total, coalesce(json_agg(_postgrest_t), '[]') AS body, nullif(current_setting('response.headers', true), '') AS response_headers, nullif(current_setting('response.status', true), '') AS response_status, '' AS response_inserted FROM ( SELECT * FROM pgrst_source ) _postgrest_t`
const readProducts = `WITH pgrst_source AS ( SELECT "api"."products".* FROM "api"."products" WHERE  "api"."products"."id" = $1    )  SELECT null::bigint AS total_result_set, pg_catalog.count(_postgrest_t) AS page_total, coalesce(json_agg(_postgrest_t), '[]') AS body, nullif(current_setting('response.headers', true), '') AS response_headers, nullif(current_setting('response.status', true), '') AS response_status, '' AS response_inserted FROM ( SELECT * FROM pgrst_source ) _postgrest_t`
const setCfg = `select set_config('search_path', $1, true), set_config('role', $2, true), set_config('request.jwt.claims', $3, true), set_config('request.method', $4, true), set_config('request.path', $5, true), set_config('request.headers', $6, true), set_config('request.cookies', $7, true)`

var stmts = map[string]string{
	"begin": "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", "beginrw": "BEGIN", "commit": "COMMIT", "rollback": "ROLLBACK",
	"set": setCfg, "rprof": readProfiles, "rprod": readProducts, "savepoint": "SAVEPOINT s1",
	"setlocalrole": "SET LOCAL ROLE authenticated",
}

type persona struct {
	name    string
	role    string
	claims  string
	headers string
	session []string // session-level statements run once at connect (simple protocol)
	kind    string   // normal | tworeads | rw | badkey | savepoint | setlocalrole
}

func claims(sub string, extra string) string {
	return `{"sub":"` + sub + `","role":"authenticated","iat":1763200000,"exp":1797760000` + extra + `}`
}

var personas = []persona{
	{name: "userA", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "normal"},
	{name: "userB", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"t"}`, kind: "normal"},
	{name: "userA_otherclaim", role: "authenticated", claims: claims("1", `,"tenant":"x"`), headers: `{"user-agent":"t"}`, kind: "normal"},
	{name: "userB_hdrs", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"other"}`, kind: "normal"},
	{name: "anon", role: "anon", claims: `{"role":"anon"}`, headers: `{"user-agent":"t"}`, kind: "normal"},
	{name: "userA_sessHeaders", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "normal",
		session: []string{`SELECT set_config('response.headers', '[{"X-Leak":"A"}]', false)`}},
	{name: "userB_sessStatus", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"t"}`, kind: "normal",
		session: []string{`SELECT set_config('response.status', '418', false)`}},
	{name: "userB_sessGUC", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"t"}`, kind: "normal",
		session: []string{`SET TimeZone = 'Asia/Tokyo'`, `SET DateStyle = 'SQL, DMY'`}},
	{name: "userA_sessClaims", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "normal",
		session: []string{`SELECT set_config('request.jwt.claims', '{"sub":"2"}', false)`}}, // overridden locally: must not matter
	{name: "userA_tworeads", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "tworeads"},
	{name: "userB_rw", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"t"}`, kind: "rw"},
	{name: "userA_badkey", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "badkey"},
	{name: "userB_savepoint", role: "authenticated", claims: claims("2", ""), headers: `{"user-agent":"t"}`, kind: "savepoint"},
	{name: "userA_lateprep", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "lateprep"},
	{name: "userA_setlocalrole", role: "authenticated", claims: claims("1", ""), headers: `{"user-agent":"t"}`, kind: "setlocalrole"},
}

// profile ids owned by users 1 and 2 plus others; products small ids
var profKeys = []string{"1", "2", "2001", "2002", "4001", "4002", "3", "5", "6001", "6002", "99999", "100001"}

type conn struct{ c *pgconn.PgConn }

func connect(port int) *conn {
	pw, err := os.ReadFile("/workspace/coalesce/secrets/authenticator.pw")
	if err != nil {
		panic(err)
	}
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=authenticator dbname=rest sslmode=require", port)
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	cfg.Password = strings.TrimSpace(string(pw))
	c, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		panic(fmt.Sprintf("connect %d: %v", port, err))
	}
	ctx := context.Background()
	// PostgREST-like session setup
	c.Exec(ctx, "SET client_encoding = 'UTF8';SET client_min_messages TO WARNING;").ReadAll()
	// PostgREST prepares reads inside a transaction after switching role
	c.Exec(ctx, "BEGIN; SELECT set_config('role', 'authenticated', true)").ReadAll()
	for n, q := range stmts {
		if _, err := c.Prepare(ctx, n, q, nil); err != nil {
			panic(fmt.Sprintf("prepare %s: %v", n, err))
		}
	}
	c.Exec(ctx, "COMMIT").ReadAll()
	return &conn{c}
}

func (c *conn) run(name string, params ...string) string {
	ps := make([][]byte, len(params))
	for i, p := range params {
		ps[i] = []byte(p)
	}
	r := c.c.ExecPrepared(context.Background(), name, ps, nil, []int16{1}).Read()
	var sb strings.Builder
	if r.Err != nil {
		if pe, ok := r.Err.(*pgconn.PgError); ok {
			return "ERR " + pe.Code + " " + pe.Message
		}
		return "ERR " + r.Err.Error()
	}
	for _, row := range r.Rows {
		for _, v := range row {
			if v == nil {
				sb.WriteString("<null>|")
			} else {
				sb.WriteString(string(v) + "|")
			}
		}
		sb.WriteString("\n")
	}
	sb.WriteString(r.CommandTag.String())
	return sb.String()
}

// parseOnly sends Parse+Sync with no Describe, exactly like PostgREST/hasql does
func (c *conn) parseOnly(name, q string) string {
	fe := c.c.Frontend()
	fe.Send(&pgproto3.Parse{Name: name, Query: q})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		return "ERR flush"
	}
	out := ""
	for {
		m, err := c.c.ReceiveMessage(context.Background())
		if err != nil {
			return out + "ERR " + err.Error()
		}
		switch x := m.(type) {
		case *pgproto3.ParseComplete:
			out += "parsed;"
		case *pgproto3.ErrorResponse:
			out += "ERR " + x.Code + " " + x.Message + ";"
		case *pgproto3.ReadyForQuery:
			return out
		}
	}
}

func (c *conn) txStatus() byte { return c.c.TxStatus() }

// one PostgREST-style request; returns transcript
func request(c *conn, p persona, key string, prod bool, bad bool, key2 string) string {
	read := "rprof"
	if prod {
		read = "rprod"
	}
	var t []string
	add := func(s string) { t = append(t, s+fmt.Sprintf(" [%c]", c.txStatus())) }
	switch p.kind {
	case "rw":
		add(c.run("beginrw"))
	case "setlocalrole":
		add(c.run("begin"))
		add(c.run("setlocalrole"))
	default:
		add(c.run("begin"))
	}
	add(c.run("set", `"api", "public"`, p.role, p.claims, "GET", "/profiles", p.headers, "{}"))
	if p.kind == "savepoint" {
		add(c.run("savepoint"))
	}
	if p.kind == "lateprep" {
		// regression: Parse-only group inside a coalescible tx, then a deviating Bind (txc fallback replays the Parse)
		add(c.parseOnly("lp", "SELECT current_setting('request.method', true), $1::int + 1"))
		add(c.run("lp", "41"))
	}
	if p.kind == "badkey" && bad {
		key = "abc"
	}
	add(c.run(read, key))
	if p.kind == "tworeads" {
		add(c.run(read, key2))
	}
	if c.txStatus() == 'E' {
		add(c.run("rollback"))
	} else {
		add(c.run("commit"))
	}
	if p.kind == "lateprep" {
		if err := c.c.Deallocate(context.Background(), "lp"); err != nil {
			add("DEALLOCERR " + err.Error())
		}
	}
	return strings.Join(t, "\n")
}

func main() {
	dur := flag.Duration("dur", 30*time.Second, "")
	workers := flag.Int("w", 6, "workers per persona")
	proxyPort := flag.Int("proxy", 56610, "")
	directPort := flag.Int("direct", 55433, "")
	seed := flag.Int64("seed", 1, "")
	flag.Parse()
	rand.Seed(*seed)
	var ops, mism atomic.Int64
	var exMu sync.Mutex
	var examples []map[string]string
	per := map[string]*atomic.Int64{}
	for _, p := range personas {
		per[p.name] = &atomic.Int64{}
	}
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for _, p := range personas {
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func(p persona, w int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(*seed*1000 + int64(w) + int64(len(p.name))*7919))
				px, dr := connect(*proxyPort), connect(*directPort)
				for _, s := range p.session {
					px.c.Exec(context.Background(), s).ReadAll()
					dr.c.Exec(context.Background(), s).ReadAll()
				}
				for time.Now().Before(deadline) {
					// heavy overlap on a few keys so personas share batch windows
					var key string
					prod := rng.Intn(4) == 0
					if prod {
						key = fmt.Sprint(1 + int(rng.ExpFloat64()*3)%20)
					} else {
						key = profKeys[int(rng.ExpFloat64()*2.5)%len(profKeys)]
					}
					bad := rng.Intn(3) == 0
					key2 := profKeys[rng.Intn(len(profKeys))]
					got := request(px, p, key, prod, bad, key2)
					want := request(dr, p, key, prod, bad, key2)
					ops.Add(1)
					per[p.name].Add(1)
					if got != want {
						mism.Add(1)
						exMu.Lock()
						if len(examples) < 8 {
							examples = append(examples, map[string]string{"persona": p.name, "key": key, "got": got, "want": want})
						}
						exMu.Unlock()
					}
				}
				px.c.Close(context.Background())
				dr.c.Close(context.Background())
			}(p, w)
		}
	}
	wg.Wait()
	pc := map[string]int64{}
	for k, v := range per {
		pc[k] = v.Load()
	}
	out, _ := json.MarshalIndent(map[string]any{"requests": ops.Load(), "mismatches": mism.Load(), "examples": examples, "per_persona": pc}, "", " ")
	fmt.Println(string(out))
}
