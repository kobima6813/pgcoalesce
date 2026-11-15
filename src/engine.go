package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ---------- server connection helpers ----------

type ioErr struct{ error }

// read until ReadyForQuery, calling fn for each message (message valid only during fn)
func (b *bconn) readUntilRFQ(fn func(pgproto3.BackendMessage)) error {
	for {
		m, err := b.fe.Receive()
		if err != nil {
			b.broken = true
			return ioErr{err}
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
		if ps, ok := m.(*pgproto3.ParameterStatus); ok {
			b.params[ps.Name] = ps.Value // e.g. client_encoding after a replayed session SET
		}
		fn(m)
	}
}

func copyErr(e *pgproto3.ErrorResponse) *pgproto3.ErrorResponse {
	c := *e
	c.UnknownFields = nil
	return &c
}

func copyRow(vals [][]byte) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		if v != nil {
			out[i] = append(make([]byte, 0, len(v)), v...)
		}
	}
	return out
}

// describe statement text with unnamed statement
func (b *bconn) describe(sql string, oids []uint32) ([]uint32, []pgproto3.FieldDescription, *pgproto3.ErrorResponse, error) {
	b.fe.Send(&pgproto3.Parse{Name: "", Query: sql, ParameterOIDs: oids})
	b.fe.Send(&pgproto3.Describe{ObjectType: 'S', Name: ""})
	b.fe.Send(&pgproto3.Sync{})
	if err := b.fe.Flush(); err != nil {
		b.broken = true
		return nil, nil, nil, ioErr{err}
	}
	var poids []uint32
	var fields []pgproto3.FieldDescription
	var perr *pgproto3.ErrorResponse
	err := b.readUntilRFQ(func(m pgproto3.BackendMessage) {
		switch x := m.(type) {
		case *pgproto3.ParameterDescription:
			poids = append([]uint32(nil), x.ParameterOIDs...)
		case *pgproto3.RowDescription:
			for _, f := range x.Fields {
				f.Name = append([]byte(nil), f.Name...)
				fields = append(fields, f)
			}
		case *pgproto3.ErrorResponse:
			perr = copyErr(x)
		}
	})
	return poids, fields, perr, err
}

func (b *bconn) simpleRows(sql string) ([][]string, *pgproto3.ErrorResponse, error) {
	b.fe.Send(&pgproto3.Query{String: sql})
	if err := b.fe.Flush(); err != nil {
		b.broken = true
		return nil, nil, ioErr{err}
	}
	var rows [][]string
	var perr *pgproto3.ErrorResponse
	err := b.readUntilRFQ(func(m pgproto3.BackendMessage) {
		switch x := m.(type) {
		case *pgproto3.DataRow:
			r := make([]string, len(x.Values))
			for i, v := range x.Values {
				r[i] = string(v)
			}
			rows = append(rows, r)
		case *pgproto3.ErrorResponse:
			perr = copyErr(x)
		}
	})
	return rows, perr, err
}

// ---------- engine (one per settings key + session fingerprint) ----------

type tmpl struct {
	id           int64
	gen          int64
	ok           bool
	why          string
	sp           *spec
	sql          string // template SQL ($1)
	rewritten    string
	rewrittenPos string
	paramOID     uint32
	fields       []pgproto3.FieldDescription
	oids         []uint32
	src          string // statement text as classified
}

type engine struct {
	key         string
	user        string
	startup     map[string]string
	fp          string // fingerprint of effective session settings (role/db settings, search_path, ...)
	pool        chan *bconn
	lanes       int
	pstatus     map[string]string
	gen         atomic.Int64 // template generation; bumped by template_ttl / reload
	stale       atomic.Bool  // settings changed (ALTER ROLE ...); new clients go to a fresh engine
	refs        atomic.Int64
	idleSince   atomic.Int64 // unix nanoseconds when the last client left; meaningful while refs == 0
	retiring    atomic.Bool  // its server connections are being closed
	mu          sync.Mutex
	tmpls       map[string]*tmpl
	batchers    map[string]*batcher
	nextID      int64
	sessionSets [][2]string
	roleCfg     string
	watchMu     sync.Mutex
	watchNames  []string // placeholder GUCs that policies/functions/views may read (nil = not computed)
	watchOK     bool     // false: dynamic current_setting() use found; transaction coalescing disabled
	watchAt     time.Time
	// transaction-template coalescing
	txTmpls    map[string]*txTmpl
	txBatchers map[string]*txBatcher
	setMemo    map[setMemoKey][]byte
	phMemo     map[string]bool
}

var (
	engMu   sync.Mutex
	engines = map[string]*engine{}
	allEngs = map[*engine]bool{}
)

func settingsKey(p map[string]string) string {
	keys := make([]string, 0, len(p))
	for k := range p {
		if k == "application_name" || k == "fallback_application_name" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k + "=" + p[k] + "\x00")
	}
	return sb.String()
}

// stateQuery fingerprints everything in a session that can change the result of a statement:
// identity (current_user/session_user incl. SET ROLE / SET SESSION AUTHORIZATION), database, every
// setting visible in pg_settings (search_path, TimeZone, DateStyle, IntervalStyle, extra_float_digits,
// bytea_output, client_encoding, lc_*, row_security, custom app.* GUCs, ...) except application_name,
// and the number of temp relations (pg_temp shadows tables). Fully qualified, operator-free where the
// operator could be shadowed, so a client's search_path cannot redefine the check.
const stateQuery = `SELECT pg_catalog.md5(pg_catalog.concat_ws(E'\n', current_user, session_user, pg_catalog.current_database(),
 (SELECT pg_catalog.string_agg(pg_catalog.concat(s.name, '=', CASE WHEN s.name OPERATOR(pg_catalog.=) 'application_name' THEN '' ELSE s.setting END), E'\n' ORDER BY s.name) FROM pg_catalog.pg_settings s),
 (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema())))`

const fpQuery = stateQuery

// verification on a client's own session: state hash, its session-level settings (to build a matching
// engine variant), and the values of watched placeholder settings.
func verifyQuery(names []string) string {
	var sb strings.Builder
	sb.WriteString(strings.TrimSuffix(strings.TrimSpace(stateQuery), ";"))
	sb.WriteString(`, (SELECT pg_catalog.json_agg(pg_catalog.json_build_array(s.name, s.setting) ORDER BY s.name) FROM pg_catalog.pg_settings s WHERE s.source OPERATOR(pg_catalog.=) 'session' AND s.name OPERATOR(pg_catalog.<>) 'application_name')::text`)
	for _, n := range names {
		sb.WriteString(", pg_catalog.current_setting(" + sqlLit(n) + ", true)")
	}
	return sb.String()
}

func credsFor(user string) *creds {
	credMu.Lock()
	defer credMu.Unlock()
	return liveCreds[user]
}

func (e *engine) dial() (*bconn, string, error) {
	b, err := dialServer(cfg.ServerAddr, e.startup, credsFor(e.user))
	if err != nil {
		return nil, "", err
	}
	stServerDials.Add(1)
	// variant engines reproduce a client's session-level SETs; the fingerprint below proves the result
	for _, kv := range e.sessionSets {
		_, perr, err := b.simpleRows("SELECT pg_catalog.set_config(" + sqlLit(kv[0]) + ", " + sqlLit(kv[1]) + ", false)")
		if err != nil || perr != nil {
			b.conn.Close()
			return nil, "", errors.New("cannot reproduce session setting " + kv[0])
		}
	}
	rows, perr, err := b.simpleRows(fpQuery)
	if err != nil || perr != nil || len(rows) != 1 {
		b.conn.Close()
		if err == nil {
			err = errors.New("fingerprint query failed")
		}
		return nil, "", err
	}
	return b, rows[0][0], nil
}

var tmplIDs atomic.Int64 // global: prepared-statement names never collide across engines

func getEngine(startup map[string]string) (*engine, error) { return getEngineSets(startup, nil) }

func setsKey(sets [][2]string) string {
	var sb strings.Builder
	for _, kv := range sets {
		sb.WriteString("\x02" + kv[0] + "=" + kv[1])
	}
	return sb.String()
}

var errEngineCap = errors.New("max_engines reached")

// liveEnginesLocked counts engines that still hold server connections (engMu held).
func liveEnginesLocked() int {
	n := 0
	for e := range allEngs {
		if !e.retiring.Load() {
			n++
		}
	}
	return n
}

// retireIdleLocked closes engines no client is attached to: every one idle for longer than maxIdle
// (maxIdle > 0), plus the longest-idle ones until `need` engines have been closed. engMu held; a
// client can only attach to an engine under engMu, so an engine seen here with refs == 0 stays unused.
func retireIdleLocked(maxIdle time.Duration, need int) int {
	var idle []*engine
	for e := range allEngs {
		if !e.retiring.Load() && e.refs.Load() == 0 {
			idle = append(idle, e)
		}
	}
	sort.Slice(idle, func(i, j int) bool { return idle[i].idleSince.Load() < idle[j].idleSince.Load() })
	now := time.Now().UnixNano()
	closed := 0
	for _, e := range idle {
		expired := maxIdle > 0 && now-e.idleSince.Load() > int64(maxIdle)
		if !expired && closed >= need {
			continue
		}
		e.stale.Store(true)
		if engines[e.key] == e {
			delete(engines, e.key)
		}
		e.maybeRetire()
		closed++
	}
	return closed
}

// engineJanitor closes the server connections of engines that have had no client for engine_idle_timeout.
// Without it every distinct session configuration ever seen (for example one search_path per tenant)
// kept `lanes` server connections open for the lifetime of the proxy.
func engineJanitor() {
	if cfg.EngineIdle <= 0 {
		return
	}
	every := cfg.EngineIdle / 2
	if every > 5*time.Second {
		every = 5 * time.Second
	}
	if every < 200*time.Millisecond {
		every = 200 * time.Millisecond
	}
	for {
		time.Sleep(every)
		engMu.Lock()
		stEngineIdleClosed.Add(int64(retireIdleLocked(cfg.EngineIdle, 0)))
		engMu.Unlock()
	}
}

func getEngineSets(startup map[string]string, sets [][2]string) (*engine, error) {
	k := settingsKey(startup) + setsKey(sets)
	engMu.Lock()
	defer engMu.Unlock()
	if e, ok := engines[k]; ok && !e.stale.Load() {
		// ALTER ROLE/DATABASE ... SET applies to new sessions: a new client must not be served by
		// connections created under older settings. One cheap catalog read per client connection.
		if rc, err := e.roleConfig(); err == nil && rc == e.roleCfg {
			e.refs.Add(1)
			return e, nil
		}
		e.stale.Store(true)
		e.maybeRetire()
	}
	// bound on engines (each holds `lanes` server connections): make room by closing the longest-idle
	// ones. If every engine is in use, a session-settings variant is refused - that client simply stays
	// on passthrough, which is always correct. An engine for a newly connecting client is still created,
	// so reaching the limit never turns clients away.
	if cfg.MaxEngines > 0 {
		if n := liveEnginesLocked(); n >= cfg.MaxEngines {
			stEngineIdleClosed.Add(int64(retireIdleLocked(0, n-cfg.MaxEngines+1)))
			if len(sets) > 0 && liveEnginesLocked() >= cfg.MaxEngines {
				stEngineCapHits.Add(1)
				return nil, errEngineCap
			}
		}
	}
	sp := map[string]string{}
	for kk, v := range startup {
		if kk != "application_name" && kk != "fallback_application_name" {
			sp[kk] = v
		}
	}
	e := &engine{key: k, sessionSets: sets, user: startup["user"], startup: sp, pool: make(chan *bconn, cfg.Lanes), lanes: cfg.Lanes, tmpls: map[string]*tmpl{}, batchers: map[string]*batcher{}}
	if rc, err := e.roleConfigOnNew(); err == nil {
		e.roleCfg = rc
	} else {
		return nil, err
	}
	for i := 0; i < cfg.Lanes; i++ {
		b, fp, err := e.dial()
		if err != nil {
			for len(e.pool) > 0 {
				(<-e.pool).close()
			}
			return nil, err
		}
		if i == 0 {
			e.pstatus = map[string]string{} // own copy: the connection keeps updating its map
			for pk, pv := range b.params {
				e.pstatus[pk] = pv
			}
			e.fp = fp
		} else if fp != e.fp {
			i-- // settings changed during setup; retry
			b.close()
			continue
		}
		e.pool <- b
	}
	if old, ok := engines[k]; ok {
		old.stale.Store(true)
		old.maybeRetire()
	}
	engines[k] = e
	allEngs[e] = true
	stEnginesLive.Add(1)
	e.refs.Add(1)
	if cfg.TemplateTTL > 0 {
		go e.ttlLoop()
	}
	return e, nil
}

const roleCfgQuery = `SELECT coalesce(pg_catalog.string_agg(pg_catalog.array_to_string(s.setconfig, E'\x01'), E'\x02' ORDER BY s.setdatabase, s.setrole), '')
FROM pg_catalog.pg_db_role_setting s
WHERE s.setrole IN (0, (SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname OPERATOR(pg_catalog.=) session_user))
  AND s.setdatabase IN (0, (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()))`

func (e *engine) roleConfig() (string, error) {
	b := <-e.pool
	defer func() { e.pool <- b }()
	if b.broken {
		return "", errors.New("broken")
	}
	rows, perr, err := b.simpleRows(roleCfgQuery)
	if err != nil || perr != nil || len(rows) != 1 {
		return "", errors.New("role config query failed")
	}
	return rows[0][0], nil
}

// on a fresh engine: read the settings catalog on a throwaway connection before dialing the pool,
// so the recorded value can only be older (never newer) than what the pooled connections saw
func (e *engine) roleConfigOnNew() (string, error) {
	b, err := dialServer(cfg.ServerAddr, e.startup, credsFor(e.user))
	if err != nil {
		return "", err
	}
	defer b.close()
	rows, perr, err := b.simpleRows(roleCfgQuery)
	if err != nil || perr != nil || len(rows) != 1 {
		return "", errors.New("role config query failed")
	}
	return rows[0][0], nil
}

func (e *engine) release() {
	if e.refs.Add(-1) == 0 {
		e.idleSince.Store(time.Now().UnixNano())
		if e.stale.Load() {
			e.maybeRetire()
		}
	}
}

// close server connections of a stale engine nobody uses any more
func (e *engine) maybeRetire() {
	if e.refs.Load() != 0 || !e.stale.Load() {
		return
	}
	if e.retiring.Swap(true) {
		return // already being closed (a second pass would wait forever for connections)
	}
	stEnginesLive.Add(-1)
	go func() {
		for i := 0; i < e.lanes; i++ {
			b := <-e.pool
			b.close()
		}
		engMu.Lock()
		delete(allEngs, e)
		if engines[e.key] == e {
			delete(engines, e.key)
		}
		engMu.Unlock()
	}()
}

func (e *engine) ttlLoop() {
	t := time.NewTicker(cfg.TemplateTTL)
	defer t.Stop()
	for range t.C {
		if e.stale.Load() && e.refs.Load() == 0 {
			return
		}
		e.invalidate()
	}
}

// drop cached templates (schema may have changed); statements re-classify lazily via gen check
func (e *engine) invalidate() {
	e.mu.Lock()
	e.tmpls = map[string]*tmpl{}
	e.batchers = map[string]*batcher{}
	e.txTmpls = nil
	e.txBatchers = nil
	e.setMemo = nil
	e.phMemo = nil
	e.gen.Add(1)
	e.mu.Unlock()
}

// reload (SIGHUP / admin): retire every engine; existing clients keep their (old-settings) engine,
// new clients get fresh connections that see ALTER ROLE / ALTER DATABASE changes.
func reloadEngines() {
	engMu.Lock()
	for e := range allEngs {
		e.stale.Store(true)
		e.invalidate()
		e.maybeRetire()
	}
	engMu.Unlock()
}

func (e *engine) getConn() *bconn {
	b := <-e.pool
	if !b.broken && cfg.ServerLifetime > 0 && time.Since(b.born) > cfg.ServerLifetime {
		nb, fp, err := e.dial()
		if err == nil && fp == e.fp {
			b.close()
			stRotations.Add(1)
			return nb
		}
		if err == nil {
			// role/database settings changed: keep the old connection for existing clients, retire engine
			nb.close()
			b.born = time.Now()
			if !e.stale.Swap(true) {
				log.Printf("settings for %q changed (ALTER ROLE/DATABASE?); new clients get a fresh engine", e.user)
			}
		}
		return b
	}
	if b.broken {
		b.conn.Close()
		for i := 0; ; i++ {
			nb, fp, err := e.dial()
			if err == nil {
				if fp != e.fp {
					// cannot reproduce the old session settings: serve correctly (per-key via fresh conn)
					// but mark stale so new clients move on
					e.stale.Store(true)
				}
				return nb
			}
			if i%50 == 0 {
				log.Printf("reconnect: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return b
}
func (e *engine) putConn(b *bconn) { e.pool <- b }

func oidsKey(o []uint32) string {
	var sb strings.Builder
	for _, x := range o {
		sb.WriteString(strconv.FormatUint(uint64(x), 10) + ",")
	}
	return sb.String()
}

func sqlLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

const firstNormalOID = 16384

var tmplDisabled = &tmpl{ok: false, why: "disabled", gen: -1}

// classify returns template info (cached per generation).
func (e *engine) classify(sql string, oids []uint32) *tmpl {
	if !cfg.Coalesce {
		return tmplDisabled
	}
	k := sql + "\x00" + oidsKey(oids)
	e.mu.Lock()
	if t, ok := e.tmpls[k]; ok {
		e.mu.Unlock()
		return t
	}
	g := e.gen.Load()
	e.mu.Unlock()
	t := e.classifyNow(sql, oids)
	t.gen = g
	t.oids = oids
	t.src = sql
	e.mu.Lock()
	if old, ok := e.tmpls[k]; ok {
		t = old
	} else {
		t.id = tmplIDs.Add(1)
		if e.gen.Load() == g {
			e.tmpls[k] = t
		}
		if t.ok {
			stTmplOK.Add(1)
		} else {
			stTmplNo.Add(1)
		}
	}
	e.mu.Unlock()
	return t
}

// fresh returns a current-generation template for a statement's cached one
func (e *engine) fresh(t *tmpl) *tmpl {
	if t == nil || t.gen == e.gen.Load() || t == tmplDisabled {
		return t
	}
	return e.classify(t.src, t.oids)
}

func (e *engine) classifyNow(sql string, oids []uint32) *tmpl {
	sp := matchSQL(sql)
	if sp == nil {
		return &tmpl{why: "shape", sql: sql}
	}
	if sp.litKind != 0 {
		return &tmpl{why: "literal-in-extended"}
	}
	if len(oids) > 1 {
		return &tmpl{why: "oids"}
	}
	b := e.getConn()
	defer e.putConn(b)
	poids, fields, perr, err := b.describe(sql, oids)
	if err != nil || perr != nil || len(poids) != 1 || len(fields) == 0 {
		return &tmpl{why: "describe"}
	}
	T := poids[0]
	q := fmt.Sprintf(`SELECT c.relkind, c.relpersistence, c.relrowsecurity::text,
 EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts=1 AND i.indkey[0]=a.attnum)::text,
 current_setting('default_transaction_isolation'), c.oid::text, (SELECT quote_ident(n.nspname)||'.'||quote_ident(t.typname) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.oid=%d),
 coalesce((SELECT o.oprnamespace = 'pg_catalog'::regnamespace AND p.provolatile = 'i' FROM pg_operator o JOIN pg_proc p ON p.oid = o.oprcode
   WHERE o.oid = to_regoperator(format('=(%%s,%%s)', a.atttypid::regtype, %d::regtype))), false)::text
FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
WHERE c.oid = to_regclass(%s) AND a.attname = %s AND NOT a.attisdropped AND a.attnum > 0`, T, T, sqlLit(sp.table), sqlLit(sp.keyName))
	rows, perr, err := b.simpleRows(q)
	if err != nil || perr != nil || len(rows) != 1 {
		return &tmpl{why: "catalog"}
	}
	r := rows[0]
	if !(r[0] == "r" || r[0] == "p") || r[1] == "t" || r[2] != "false" || r[3] != "true" || r[4] != "read committed" {
		return &tmpl{why: "catalog:" + strings.Join(r[:5], "/")}
	}
	// fail closed: every output column must be a plain column of this very table (no attribute-notation
	// functions, no composite field access, nothing computed)
	relOID, _ := strconv.ParseUint(r[5], 10, 32)
	for _, f := range fields {
		if f.TableOID != uint32(relOID) || f.TableAttributeNumber <= 0 {
			return &tmpl{why: "non-column-output"}
		}
		// built-in types only: no user-defined output functions that could read session state
		if f.DataTypeOID >= firstNormalOID {
			return &tmpl{why: "non-builtin-type"}
		}
	}
	if T >= firstNormalOID {
		return &tmpl{why: "non-builtin-key-type"}
	}
	// the "=" the server resolves for key = $1 must be the immutable pg_catalog one
	if r[7] != "true" {
		return &tmpl{why: "operator"}
	}
	r = append(r[:5], r[6])
	rw := sp.rewrite(r[5])
	_, rf, perr, err := b.describe(rw, nil)
	if err != nil || perr != nil || len(rf) != len(fields)+1 {
		return &tmpl{why: "rewrite"}
	}
	for i, f := range fields {
		g := rf[i+1]
		if string(f.Name) != string(g.Name) || f.DataTypeOID != g.DataTypeOID || f.TypeModifier != g.TypeModifier || f.TableOID != g.TableOID || f.TableAttributeNumber != g.TableAttributeNumber {
			return &tmpl{why: "rewrite-mismatch"}
		}
	}
	rwp := sp.rewritePos(r[5])
	_, rfp, perr, err := b.describe(rwp, nil)
	if err != nil || perr != nil || len(rfp) != len(fields)+1 {
		rwp = "" // fall back to join form only
	} else {
		for i, f := range fields {
			g := rfp[i+1]
			if string(f.Name) != string(g.Name) || f.DataTypeOID != g.DataTypeOID || f.TypeModifier != g.TypeModifier || f.TableOID != g.TableOID || f.TableAttributeNumber != g.TableAttributeNumber {
				rwp = ""
				break
			}
		}
	}
	return &tmpl{ok: true, sp: sp, sql: sql, rewritten: rw, rewrittenPos: rwp, paramOID: T, fields: fields}
}

// ---------- batching ----------

type result struct {
	rows [][][]byte
	err  *pgproto3.ErrorResponse
}

type req struct {
	pfmt  int16
	param []byte
	t0    time.Time
	done  chan result
}

type batcher struct {
	e        *engine
	t        *tmpl
	pfmt     int16
	rfmts    []int16
	mu       sync.Mutex
	pending  []*req
	inflight int
	timer    bool
	arr      [32]time.Time
	ai       int
}

func normFormats(f []int16, n int) []int16 {
	out := make([]int16, n)
	if len(f) == 1 {
		for i := range out {
			out[i] = f[0]
		}
	} else if len(f) == n {
		copy(out, f)
	}
	return out
}

func (e *engine) batcherFor(t *tmpl, pfmt int16, rfmts []int16) *batcher {
	var sb strings.Builder
	sb.WriteString(strconv.FormatInt(t.id, 10) + "/" + strconv.Itoa(int(pfmt)) + "/")
	for _, f := range rfmts {
		sb.WriteByte(byte('0' + f))
	}
	k := sb.String()
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.batchers[k]
	if !ok {
		b = &batcher{e: e, t: t, pfmt: pfmt, rfmts: rfmts}
		e.batchers[k] = b
	}
	return b
}

var errCanceled = &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "57014", Message: "canceling statement due to user request"}

func (e *engine) submit(t *tmpl, pfmt int16, param []byte, rfmts []int16, cancel chan struct{}) result {
	b := e.batcherFor(t, pfmt, rfmts)
	r := &req{pfmt: pfmt, param: param, t0: time.Now(), done: make(chan result, 1)}
	stCoalescedReqs.Add(1)
	b.mu.Lock()
	b.pending = append(b.pending, r)
	if int64(len(b.pending)) > stQueueMax.Load() {
		stQueueMax.Store(int64(len(b.pending)))
	}
	b.arr[b.ai%len(b.arr)] = r.t0
	b.ai++
	b.scheduleLocked()
	b.mu.Unlock()
	select {
	case res := <-r.done:
		hLatency.observe(time.Since(r.t0).Seconds())
		return res
	case <-cancel:
		// only this client's request is abandoned; the shared batch still completes for the others
		stCancelLocal.Add(1)
		return result{err: errCanceled}
	}
}

// expected number of additional arrivals within the window, from recent arrival rate
func (b *batcher) expectedLocked(now time.Time) float64 {
	n := b.ai
	if n > len(b.arr) {
		n = len(b.arr)
	}
	if n < 2 {
		return 0
	}
	oldest := b.arr[(b.ai-n)%len(b.arr)]
	span := now.Sub(oldest).Seconds()
	if span <= 0 {
		return 1e9
	}
	rate := float64(n-1) / span
	return rate * cfg.Window.Seconds()
}

func (b *batcher) scheduleLocked() {
	if len(b.pending) == 0 || b.inflight >= b.e.lanes {
		return // lane completion will reschedule; backlog grows into bigger batches
	}
	now := time.Now()
	if len(b.pending) >= cfg.MaxBatch || cfg.Window == 0 || b.expectedLocked(now) < cfg.MinExpected {
		b.flushLocked()
		return
	}
	deadline := b.pending[0].t0.Add(cfg.Window)
	if !now.Before(deadline) {
		b.flushLocked()
		return
	}
	if !b.timer {
		b.timer = true
		time.AfterFunc(deadline.Sub(now), func() {
			b.mu.Lock()
			b.timer = false
			if len(b.pending) > 0 && b.inflight < b.e.lanes {
				b.flushLocked()
			}
			b.mu.Unlock()
		})
	}
}

func (b *batcher) flushLocked() {
	n := len(b.pending)
	if n > cfg.MaxBatch {
		n = cfg.MaxBatch
	}
	batch := append([]*req(nil), b.pending[:n]...)
	b.pending = append(b.pending[:0], b.pending[n:]...)
	b.inflight++
	go func() {
		b.run(batch)
		b.mu.Lock()
		b.inflight--
		b.scheduleLocked()
		b.mu.Unlock()
	}()
}

type uval struct {
	null  bool
	param []byte
	reqs  []*req
	rows  [][][]byte
	err   *pgproto3.ErrorResponse
}

func (b *batcher) run(batch []*req) {
	idx := map[string]int{}
	var us []*uval
	for _, r := range batch {
		k := "n"
		if r.param != nil {
			k = "v" + string(r.param)
		}
		i, ok := idx[k]
		if !ok {
			i = len(us)
			idx[k] = i
			us = append(us, &uval{null: r.param == nil, param: r.param})
		}
		us[i].reqs = append(us[i].reqs, r)
	}
	stBatches.Add(1)
	stBatchItems.Add(int64(len(batch)))
	stUnique.Add(int64(len(us)))
	hBatchSize.observe(float64(len(batch)))
	hBatchKeys.observe(float64(len(us)))
	c := b.e.getConn()
	ts := time.Now()
	for _, r := range batch {
		hWait.observe(ts.Sub(r.t0).Seconds())
	}
	var err error
	if len(us) == 1 {
		stSingles.Add(1)
		err = b.execSingle(c, us[0])
	} else if b.pfmt == 0 && !textArraySafe(c.params["client_encoding"]) {
		for _, u := range us {
			if err = b.execSingle(c, u); err != nil {
				break
			}
		}
	} else {
		var perr *pgproto3.ErrorResponse
		perr, err = b.execBatch(c, us)
		if err == nil && perr != nil {
			// isolate the failing value(s): run each individually so errors reach only their clients
			stFallback.Add(1)
			for _, u := range us {
				u.rows, u.err = nil, nil
				if err = b.execSingle(c, u); err != nil {
					break
				}
			}
		}
	}
	hBatchExec.observe(time.Since(ts).Seconds())
	b.e.putConn(c)
	for _, u := range us {
		res := result{rows: u.rows, err: u.err}
		if err != nil {
			res = result{err: &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "08006", Message: "coalescer backend failure: " + err.Error()}}
		}
		for _, r := range u.reqs {
			r.done <- res
		}
	}
}

func (b *batcher) ensurePrepared(c *bconn, name, sql string, oids []uint32) {
	if c.prepared[name] {
		return
	}
	c.fe.Send(&pgproto3.Parse{Name: name, Query: sql, ParameterOIDs: oids})
	c.prepared[name] = true
}

func (b *batcher) execSingle(c *bconn, u *uval) error {
	name := "cs_" + strconv.FormatInt(b.t.id, 10)
	b.ensurePrepared(c, name, b.t.sql, []uint32{b.t.paramOID})
	c.fe.Send(&pgproto3.Bind{PreparedStatement: name, ParameterFormatCodes: []int16{b.pfmt}, Parameters: [][]byte{u.param}, ResultFormatCodes: b.rfmts})
	c.fe.Send(&pgproto3.Execute{})
	c.fe.Send(&pgproto3.Sync{})
	if err := c.fe.Flush(); err != nil {
		c.broken = true
		return err
	}
	return c.readUntilRFQ(func(m pgproto3.BackendMessage) {
		switch x := m.(type) {
		case *pgproto3.DataRow:
			u.rows = append(u.rows, copyRow(x.Values))
		case *pgproto3.ErrorResponse:
			u.err = copyErr(x)
			if x.Code == "26000" { // prepared statement vanished
				delete(c.prepared, name)
			}
		}
	})
}

// textArraySafe reports whether a text-format array literal can be built by byte-wise quoting in this
// client encoding. That needs every byte of a multi-byte character to be >= 0x80, which holds for all
// encodings Postgres accepts as a server encoding. The client-only encodings (SJIS, BIG5, GBK, GB18030,
// UHC, JOHAB, SHIFT_JIS_2004) use the backslash and other ASCII bytes as trail bytes, so quoting would split a
// character. Unknown names are treated as unsafe; such batches run key by key (always correct).
func textArraySafe(enc string) bool {
	e := strings.ToUpper(enc)
	switch {
	case e == "UTF8", e == "SQL_ASCII", e == "MULE_INTERNAL":
		return true
	case strings.HasPrefix(e, "LATIN"), strings.HasPrefix(e, "WIN"), strings.HasPrefix(e, "KOI8"),
		strings.HasPrefix(e, "EUC_"), strings.HasPrefix(e, "ISO_8859_"):
		return true
	}
	return false
}

func arrayText(us []*uval) []byte {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, u := range us {
		if i > 0 {
			sb.WriteByte(',')
		}
		if u.null {
			sb.WriteString("NULL")
			continue
		}
		// byte-exact: the value is in the client's encoding, which need not be UTF-8. Decoding it as
		// UTF-8 runes (as an earlier version did) silently replaced every non-UTF-8 byte.
		sb.WriteByte('"')
		for i := 0; i < len(u.param); i++ {
			ch := u.param[i]
			if ch == '"' || ch == '\\' {
				sb.WriteByte('\\')
			}
			sb.WriteByte(ch)
		}
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return []byte(sb.String())
}

func arrayBinary(us []*uval, elemOID uint32) []byte {
	buf := make([]byte, 0, 20+len(us)*12)
	hasNull := uint32(0)
	for _, u := range us {
		if u.null {
			hasNull = 1
		}
	}
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint32(buf, hasNull)
	buf = binary.BigEndian.AppendUint32(buf, elemOID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(us)))
	buf = binary.BigEndian.AppendUint32(buf, 1)
	for _, u := range us {
		if u.null {
			buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF)
			continue
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(u.param)))
		buf = append(buf, u.param...)
	}
	return buf
}

func (b *batcher) execBatch(c *bconn, us []*uval) (*pgproto3.ErrorResponse, error) {
	name := "cq_" + strconv.FormatInt(b.t.id, 10)
	sqlText := b.t.rewritten
	pos := b.t.rewrittenPos != "" && len(us) <= cfg.PosMax
	if pos {
		name = "cp_" + strconv.FormatInt(b.t.id, 10)
		sqlText = b.t.rewrittenPos
	}
	b.ensurePrepared(c, name, sqlText, nil)
	var p []byte
	if b.pfmt == 0 {
		p = arrayText(us)
	} else {
		p = arrayBinary(us, b.t.paramOID)
	}
	rf := append([]int16{0}, b.rfmts...)
	c.fe.Send(&pgproto3.Bind{PreparedStatement: name, ParameterFormatCodes: []int16{b.pfmt}, Parameters: [][]byte{p}, ResultFormatCodes: rf})
	c.fe.Send(&pgproto3.Execute{})
	c.fe.Send(&pgproto3.Sync{})
	if err := c.fe.Flush(); err != nil {
		c.broken = true
		return nil, err
	}
	var perr *pgproto3.ErrorResponse
	var bad error
	err := c.readUntilRFQ(func(m pgproto3.BackendMessage) {
		switch x := m.(type) {
		case *pgproto3.DataRow:
			if pos {
				s := strings.Trim(string(x.Values[0]), "{}")
				if s == "" {
					bad = errors.New("row without position")
					return
				}
				row := copyRow(x.Values[1:])
				for _, ps := range strings.Split(s, ",") {
					o, e := strconv.Atoi(ps)
					if e != nil || o < 1 || o > len(us) {
						bad = errors.New("bad position")
						return
					}
					us[o-1].rows = append(us[o-1].rows, row)
				}
				return
			}
			o, e := strconv.Atoi(string(x.Values[0]))
			if e != nil || o < 1 || o > len(us) {
				bad = errors.New("bad ordinal")
				return
			}
			us[o-1].rows = append(us[o-1].rows, copyRow(x.Values[1:]))
		case *pgproto3.ErrorResponse:
			perr = copyErr(x)
			if x.Code == "26000" {
				delete(c.prepared, name)
			}
		}
	})
	if err == nil && bad != nil && perr == nil {
		// routing ambiguity → let caller fall back to per-key execution (always correct)
		perr = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "coalesce routing: " + bad.Error()}
	}
	return perr, err
}
