package main

// Transaction-template coalescing (PostgREST / Supabase request shape).
//
// A client transaction of exactly this form is answered from a shared transaction:
//   BEGIN [ISOLATION LEVEL READ COMMITTED] [READ ONLY]
//   SELECT set_config(<name>, <value>, true) [, set_config(...)]...     (one or more such statements)
//   <exactly one read of a recognised point-lookup shape, one parameter>
//   COMMIT | ROLLBACK | END
// Requests are batched only when their fingerprint is identical: engine (role, database, verified
// session state) + BEGIN text + every set_config statement text and parameter bytes, in order +
// read template + formats. The shared transaction replays the same BEGIN and set_config statements,
// runs one batched read (LATERAL over the distinct keys), and commits. Anything else, any error,
// or any deviation falls back to replaying the client's transaction on its own session (fail closed).

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	txBeginRe   = regexp.MustCompile(`(?i)^\s*(BEGIN|START\s+TRANSACTION)(\s+ISOLATION\s+LEVEL\s+READ\s+COMMITTED)?(\s+READ\s+ONLY)?\s*;?\s*$`)
	txEndRe     = regexp.MustCompile(`(?i)^\s*(COMMIT|END|ROLLBACK)\s*;?\s*$`)
	setCallRe   = regexp.MustCompile(`(?i)^\s*set_config\s*\(\s*(\$[0-9]+|'[A-Za-z0-9_.]+')\s*,\s*(\$[0-9]+)\s*,\s*true\s*\)\s*$`)
	setStmtRe   = regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s*;?\s*$`)
	pgrstReadRe = regexp.MustCompile(`(?s)^WITH pgrst_source AS \( SELECT ((?:"[^"]+"\."[^"]+"\.(?:\*|"[^"]+"))(?:, (?:"[^"]+"\."[^"]+"\."[^"]+"))*) FROM ("[^"]+")\.("[^"]+") WHERE\s+"[^"]+"\."[^"]+"\.("[^"]+") = \$1\s*\)\s*(SELECT null::bigint AS total_result_set, pg_catalog\.count\(_postgrest_t\) AS page_total, coalesce\(json_agg\(_postgrest_t\), '\[\]'\) AS body, nullif\(current_setting\('response\.headers', true\), ''\) AS response_headers, nullif\(current_setting\('response\.status', true\), ''\) AS response_status, '' AS response_inserted) FROM \( SELECT \* FROM pgrst_source \) _postgrest_t$`)
)

type txLogEntry struct {
	group  []pgproto3.FrontendMessage
	expect [][]byte // DataRow values we answered with (nil if no row)
}

type txState struct {
	active   bool
	aborted  bool
	beginSQL string
	fp       []string // fingerprint parts
	sets     []setStmt
	read     bool
	log      []txLogEntry
	rolePrev string  // value of the last role set_config so far ("" if none)
	spVal    *string // value of the last search_path set_config (nil if none)
	setNames map[string]bool
}

type setStmt struct {
	sql    string
	params [][]byte
	pfmts  []int16
	oids   []uint32
}

// one extended-protocol statement execution group: [Parse] Bind [Describe] Execute (Sync handled by caller)
type xgroup struct {
	parse *pgproto3.Parse
	bind  *pgproto3.Bind
	desc  *pgproto3.Describe
	exec  *pgproto3.Execute
	sql   string
	oids  []uint32
}

func (c *client) parseXGroup(group []pgproto3.FrontendMessage) *xgroup {
	g := &xgroup{}
	for _, m := range group {
		switch x := m.(type) {
		case *pgproto3.Parse:
			if g.parse != nil || g.bind != nil {
				return nil
			}
			g.parse = x
		case *pgproto3.Bind:
			if g.bind != nil || x.DestinationPortal != "" {
				return nil
			}
			g.bind = x
		case *pgproto3.Describe:
			if g.desc != nil || g.bind == nil || x.ObjectType != 'P' || x.Name != "" {
				return nil
			}
			g.desc = x
		case *pgproto3.Execute:
			if g.exec != nil || g.bind == nil || x.Portal != "" || x.MaxRows != 0 {
				return nil
			}
			g.exec = x
		default:
			return nil
		}
	}
	if g.bind == nil || g.exec == nil {
		return nil
	}
	if g.parse != nil {
		if g.parse.Name != g.bind.PreparedStatement {
			return nil
		}
		g.sql, g.oids = g.parse.Query, g.parse.ParameterOIDs
	} else {
		c.mu.Lock()
		st := c.stmts[g.bind.PreparedStatement]
		c.mu.Unlock()
		if st == nil {
			return nil
		}
		g.sql, g.oids = st.query, st.oids
	}
	return g
}

func (c *client) txcLog(group []pgproto3.FrontendMessage, expect [][]byte) {
	cp := make([]pgproto3.FrontendMessage, len(group))
	copy(cp, group)
	c.txc.log = append(c.txc.log, txLogEntry{group: cp, expect: expect})
}

// remember a statement parsed locally (exists only in the proxy until forwarded)
func (c *client) txcNoteParse(g *xgroup) {
	if g.parse == nil {
		return
	}
	c.mu.Lock()
	c.stmts[g.parse.Name] = &stmt{query: g.parse.Query, oids: g.parse.ParameterOIDs}
	c.mu.Unlock()
}

func (c *client) reply(msgs []pgproto3.BackendMessage, ts byte) bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.send(msgs...)
	c.send(&pgproto3.ReadyForQuery{TxStatus: ts})
	return c.be.Flush() == nil
}

var setFieldDesc = pgproto3.FieldDescription{Name: []byte("set_config"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}

// tryTxc handles a Sync-terminated group. Returns (handled, ok).
func (c *client) tryTxc(group []pgproto3.FrontendMessage) (bool, bool) {
	if !cfg.Coalesce || !cfg.TxCoalesce {
		return false, true
	}
	if !c.txc.active {
		// start only from a clean, verified, idle state
		if !c.groupMayBegin(group) {
			return false, true
		}
		g := c.parseXGroup(group)
		if g == nil || !txBeginRe.MatchString(g.sql) || len(g.bind.Parameters) != 0 {
			return false, true
		}
		if !c.canLocal() || !c.stateOK() {
			return false, true
		}
		if _, wok := c.eng.watched(); !wok {
			return false, true
		}
		c.txc = txState{active: true, beginSQL: g.sql}
		c.txcNoteParse(g)
		c.txcLog(group, nil)
		stTxBegins.Add(1)
		var out []pgproto3.BackendMessage
		if g.parse != nil {
			out = append(out, &pgproto3.ParseComplete{})
		}
		out = append(out, &pgproto3.BindComplete{})
		if g.desc != nil {
			out = append(out, &pgproto3.NoData{})
		}
		out = append(out, &pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		return true, c.reply(out, 'T')
	}
	g := c.parseXGroup(group)
	if c.txc.aborted {
		if g != nil && txEndRe.MatchString(g.sql) && len(g.bind.Parameters) == 0 {
			c.txcNoteParse(g)
			c.txc = txState{}
			return true, c.reply(txSimpleReply(g, "ROLLBACK"), 'I')
		}
		var out []pgproto3.BackendMessage
		out = append(out, &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "25P02", Message: "current transaction is aborted, commands ignored until end of transaction block"})
		return true, c.reply(out, 'E')
	}
	if g == nil {
		// Parse-only group (driver preparing its next statement): answer locally, keep the statement local
		parses := 0
		for _, m := range group {
			if x, ok := m.(*pgproto3.Parse); ok && x.Name != "" {
				c.mu.Lock()
				exists := c.stmts[x.Name] != nil
				c.mu.Unlock()
				if !exists {
					parses++
					continue
				}
			}
			parses = -1
			break
		}
		if parses <= 0 {
			return true, c.txcFallback(group)
		}
		var out []pgproto3.BackendMessage
		for _, m := range group {
			x := m.(*pgproto3.Parse)
			c.mu.Lock()
			c.stmts[x.Name] = &stmt{query: x.Query, oids: x.ParameterOIDs}
			c.mu.Unlock()
			out = append(out, &pgproto3.ParseComplete{})
		}
		c.txcLog(group, nil)
		ts := byte('T')
		return true, c.reply(out, ts)
	}
	// end of transaction
	if txEndRe.MatchString(g.sql) && len(g.bind.Parameters) == 0 && c.txc.read {
		tag := strings.ToUpper(strings.Fields(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(g.sql), ";")))[0])
		if tag == "END" {
			tag = "COMMIT"
		}
		c.txcNoteParse(g)
		c.txc = txState{}
		stTxCommits.Add(1)
		return true, c.reply(txSimpleReply(g, tag), 'I')
	}
	if c.txc.read {
		return true, c.txcFallback(group) // anything after the single read
	}
	// set_config statement
	if vals, ok := c.txcSetConfig(g); ok {
		c.txcNoteParse(g)
		row := make([][]byte, len(vals))
		copy(row, vals)
		c.txcLog(group, row)
		c.txc.sets = append(c.txc.sets, setStmt{sql: g.sql, params: g.bind.Parameters, pfmts: g.bind.ParameterFormatCodes, oids: g.oids})
		var out []pgproto3.BackendMessage
		if g.parse != nil {
			out = append(out, &pgproto3.ParseComplete{})
		}
		out = append(out, &pgproto3.BindComplete{})
		rf := normFormats(g.bind.ResultFormatCodes, len(vals))
		if g.desc != nil {
			fs := make([]pgproto3.FieldDescription, len(vals))
			for i := range fs {
				fs[i] = setFieldDesc
				fs[i].Format = rf[i]
			}
			out = append(out, &pgproto3.RowDescription{Fields: fs})
		}
		out = append(out, &pgproto3.DataRow{Values: vals}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		return true, c.reply(out, 'T')
	}
	// the single read
	if len(c.txc.sets) > 0 && len(g.bind.Parameters) == 1 {
		t := c.eng.classifyTx(g.sql, g.oids, c.txc.rolePrev, c.txc.spVal)
		if t != nil && t.ok {
			return true, c.txcRead(group, g, t)
		}
	}
	return true, c.txcFallback(group)
}

func txSimpleReply(g *xgroup, tag string) []pgproto3.BackendMessage {
	var out []pgproto3.BackendMessage
	if g.parse != nil {
		out = append(out, &pgproto3.ParseComplete{})
	}
	out = append(out, &pgproto3.BindComplete{})
	if g.desc != nil {
		out = append(out, &pgproto3.NoData{})
	}
	return append(out, &pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// parse "SELECT set_config(a, $n, true), ..." and compute the exact return values without a round trip
// where provably identical (placeholder custom settings), otherwise from a memo evaluated on the server.
func (c *client) txcSetConfig(g *xgroup) ([][]byte, bool) {
	m := setStmtRe.FindStringSubmatch(g.sql)
	if m == nil || strings.ContainsAny(m[1], ";-/") {
		return nil, false
	}
	calls := splitTop(m[1])
	if len(calls) == 0 {
		return nil, false
	}
	params := g.bind.Parameters
	pf := normFormats(g.bind.ParameterFormatCodes, len(params))
	for i, o := range g.oids {
		if o != 0 && o != 25 && o != 705 && o != 1043 {
			return nil, false
		}
		_ = i
	}
	for _, f := range pf {
		if f != 0 {
			return nil, false
		}
	}
	var vals [][]byte
	role := c.txc.rolePrev
	for _, call := range calls {
		cm := setCallRe.FindStringSubmatch(call)
		if cm == nil {
			return nil, false
		}
		var name string
		if strings.HasPrefix(cm[1], "$") {
			i, _ := strconv.Atoi(cm[1][1:])
			if i < 1 || i > len(params) || params[i-1] == nil {
				return nil, false
			}
			name = string(params[i-1])
		} else {
			name = strings.Trim(cm[1], "'")
		}
		vi, _ := strconv.Atoi(cm[2][1:])
		if vi < 1 || vi > len(params) || params[vi-1] == nil {
			return nil, false
		}
		val := params[vi-1]
		if strings.EqualFold(name, "client_encoding") {
			return nil, false // changes how the key parameter is decoded inside the transaction: never share it
		}
		res, ok := c.eng.setConfigResult(role, name, val)
		if !ok {
			return nil, false
		}
		if strings.EqualFold(name, "role") {
			role = string(val)
		}
		if strings.EqualFold(name, "search_path") {
			v := string(val)
			c.txc.spVal = &v
		}
		if c.txc.setNames == nil {
			c.txc.setNames = map[string]bool{}
		}
		c.txc.setNames[strings.ToLower(name)] = true
		vals = append(vals, res)
	}
	c.txc.rolePrev = role
	return vals, true
}

func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	inq := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			inq = !inq
		case '(':
			if !inq {
				depth++
			}
		case ')':
			if !inq {
				depth--
			}
		case ',':
			if !inq && depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// ---------- engine side ----------

type setMemoKey struct{ role, name, val string }

var placeholderRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$`)

func (e *engine) setConfigResult(role, name string, val []byte) ([]byte, bool) {
	lname := strings.ToLower(name)
	// a custom (dotted) setting that no loaded module defines is a placeholder: set_config returns the
	// value verbatim. Verified once per name against pg_settings.
	if placeholderRe.MatchString(name) {
		if ph, ok := e.isPlaceholder(lname); ok && ph {
			return append([]byte(nil), val...), true
		}
	}
	k := setMemoKey{role, lname, string(val)}
	e.mu.Lock()
	if e.setMemo == nil {
		e.setMemo = map[setMemoKey][]byte{}
	}
	if r, ok := e.setMemo[k]; ok {
		e.mu.Unlock()
		return r, r != nil
	}
	e.mu.Unlock()
	b := e.getConn()
	defer e.putConn(b)
	q := "SELECT pg_catalog.set_config($1, $2, true)"
	var res []byte
	var failed bool
	b.fe.Send(&pgproto3.Query{String: "BEGIN READ ONLY"})
	if role != "" {
		b.fe.Send(&pgproto3.Parse{Query: q})
		b.fe.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte("role"), []byte(role)}})
		b.fe.Send(&pgproto3.Execute{})
	}
	b.fe.Send(&pgproto3.Parse{Query: q})
	b.fe.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte(name), val}})
	b.fe.Send(&pgproto3.Execute{})
	b.fe.Send(&pgproto3.Sync{})
	b.fe.Send(&pgproto3.Query{String: "ROLLBACK"})
	if b.fe.Flush() != nil {
		b.broken = true
		return nil, false
	}
	for i := 0; i < 3; i++ { // BEGIN RFQ, pipeline RFQ, ROLLBACK RFQ
		err := b.readUntilRFQ(func(m pgproto3.BackendMessage) {
			switch x := m.(type) {
			case *pgproto3.DataRow:
				if i == 1 && len(x.Values) == 1 && x.Values[0] != nil {
					res = append([]byte(nil), x.Values[0]...)
				}
			case *pgproto3.ErrorResponse:
				failed = true
			}
		})
		if err != nil {
			return nil, false
		}
	}
	if failed {
		res = nil
	}
	e.mu.Lock()
	if len(e.setMemo) < 10000 {
		e.setMemo[k] = res
	}
	e.mu.Unlock()
	return res, res != nil
}

func (e *engine) isPlaceholder(name string) (bool, bool) {
	e.mu.Lock()
	if e.phMemo == nil {
		e.phMemo = map[string]bool{}
	}
	if v, ok := e.phMemo[name]; ok {
		e.mu.Unlock()
		return v, true
	}
	e.mu.Unlock()
	b := e.getConn()
	rows, perr, err := b.simpleRows("SELECT count(*) FROM pg_catalog.pg_settings WHERE lower(name) OPERATOR(pg_catalog.=) " + sqlLit(name))
	e.putConn(b)
	if err != nil || perr != nil || len(rows) != 1 {
		return false, false
	}
	v := rows[0][0] == "0"
	e.mu.Lock()
	e.phMemo[name] = v
	e.mu.Unlock()
	return v, true
}

type txTmpl struct {
	id       int64
	ok       bool
	why      string
	sql      string
	oids     []uint32
	batchSQL string
	paramOID uint32
	fields   []pgproto3.FieldDescription
}

func (e *engine) classifyTx(sql string, oids []uint32, role string, sp *string) *txTmpl {
	k := sql + "\x00" + oidsKey(oids) + "\x00" + role
	if sp != nil {
		k += "\x00" + *sp
	}
	e.mu.Lock()
	if e.txTmpls == nil {
		e.txTmpls = map[string]*txTmpl{}
	}
	if t, ok := e.txTmpls[k]; ok {
		e.mu.Unlock()
		return t
	}
	e.mu.Unlock()
	t := e.classifyTxNow(sql, oids, role, sp)
	if traceOn {
		log.Printf("classifyTx ok=%v why=%s", t.ok, t.why)
	}
	e.mu.Lock()
	t.id = tmplIDs.Add(1)
	e.txTmpls[k] = t
	e.mu.Unlock()
	if t.ok {
		stTmplOK.Add(1)
	} else {
		stTmplNo.Add(1)
	}
	return t
}

func (e *engine) classifyTxNow(sql string, oids []uint32, role string, sp *string) *txTmpl {
	m := pgrstReadRe.FindStringSubmatch(sql)
	if m == nil || strings.Count(sql, "$1") != 1 || strings.Contains(sql, "$2") || len(oids) > 1 {
		return &txTmpl{why: "shape"}
	}
	schema, table, key := m[2], m[3], strings.Trim(m[4], `"`)
	b := e.getConn()
	defer e.putConn(b)
	// analyse under the same role / search_path the transactions use, then roll back
	if _, perr, err := b.simpleRows("BEGIN READ ONLY"); err != nil || perr != nil {
		return &txTmpl{why: "begin"}
	}
	defer b.simpleRows("ROLLBACK")
	if role != "" {
		if _, perr, err := b.simpleRows("SELECT pg_catalog.set_config('role', " + sqlLit(role) + ", true)"); err != nil || perr != nil {
			return &txTmpl{why: "role"}
		}
	}
	if sp != nil {
		if _, perr, err := b.simpleRows("SELECT pg_catalog.set_config('search_path', " + sqlLit(*sp) + ", true)"); err != nil || perr != nil {
			return &txTmpl{why: "search_path"}
		}
	}
	poids, fields, perr, err := b.describe(sql, oids)
	if err != nil || perr != nil || len(poids) != 1 || len(fields) == 0 {
		return &txTmpl{why: "describe"}
	}
	T := poids[0]
	if T >= firstNormalOID {
		return &txTmpl{why: "non-builtin-key-type"}
	}
	// table (RLS allowed: every request in a batch has the identical role and settings), unique key, = operator
	q := fmt.Sprintf(`SELECT c.relkind::text, c.relpersistence::text,
 EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts=1 AND i.indkey[0]=a.attnum)::text,
 (SELECT quote_ident(n.nspname)||'.'||quote_ident(t.typname) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.oid=%d),
 coalesce((SELECT o.oprnamespace = 'pg_catalog'::regnamespace AND p.provolatile = 'i' FROM pg_operator o JOIN pg_proc p ON p.oid = o.oprcode
   WHERE o.oid = to_regoperator(format('=(%%s,%%s)', a.atttypid::regtype, %d::regtype))), false)::text
FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
WHERE c.oid = to_regclass(%s) AND a.attname = %s AND NOT a.attisdropped AND a.attnum > 0`, T, T, sqlLit(schema+"."+table), sqlLit(key))
	rows, perr, err := b.simpleRows(q)
	if err != nil || perr != nil || len(rows) != 1 {
		return &txTmpl{why: "catalog"}
	}
	r := rows[0]
	if !(r[0] == "r" || r[0] == "p") || r[1] == "t" || r[2] != "true" || r[4] != "true" {
		return &txTmpl{why: "catalog:" + strings.Join(r, "/")}
	}
	// fail closed: a policy that calls a volatile (possibly side-effecting) function would run once per batch
	// row instead of once per client statement; such tables are never transaction-coalesced
	vq := fmt.Sprintf(`SELECT count(*)::text FROM pg_catalog.pg_policy pol JOIN pg_catalog.pg_depend d
  ON d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_policy'::pg_catalog.regclass AND d.objid OPERATOR(pg_catalog.=) pol.oid
  AND d.refclassid OPERATOR(pg_catalog.=) 'pg_catalog.pg_proc'::pg_catalog.regclass
 JOIN pg_catalog.pg_proc p ON p.oid OPERATOR(pg_catalog.=) d.refobjid
 WHERE pol.polrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass(%s) AND p.provolatile OPERATOR(pg_catalog.<>) 'i' AND p.provolatile OPERATOR(pg_catalog.<>) 's'`, sqlLit(schema+"."+table))
	vrows, vperr, verr := b.simpleRows(vq)
	if verr != nil || vperr != nil || len(vrows) != 1 || vrows[0][0] != "0" {
		return &txTmpl{why: "policy-volatile"}
	}
	inner := strings.Replace(sql, "$1", "k.v", 1)
	bsql := "SELECT k.ord, x.* FROM unnest($1::" + r[3] + "[]) WITH ORDINALITY AS k(v, ord) CROSS JOIN LATERAL (" + inner + ") x"
	_, bf, perr, err := b.describe(bsql, nil)
	if err != nil || perr != nil || len(bf) != len(fields)+1 {
		return &txTmpl{why: "rewrite"}
	}
	for i, f := range fields {
		g := bf[i+1]
		if string(f.Name) != string(g.Name) || f.DataTypeOID != g.DataTypeOID || f.TypeModifier != g.TypeModifier {
			return &txTmpl{why: "rewrite-mismatch"}
		}
	}
	return &txTmpl{ok: true, sql: sql, oids: oids, batchSQL: bsql, paramOID: T, fields: fields}
}

// ---------- batching of whole transactions ----------

type txReq struct {
	param []byte
	t0    time.Time
	done  chan txResult
}

type txResult struct {
	rows     [][][]byte
	fallback bool
	err      error
}

type txBatcher struct {
	e        *engine
	t        *txTmpl
	begin    string
	sets     []setStmt
	pfmt     int16
	rfmts    []int16
	wn       []string // watched settings that must have these values in the shared transaction
	wv       []*string
	mu       sync.Mutex
	pending  []*txReq
	inflight int
	timer    bool
	arr      [32]time.Time
	ai       int
}

func (c *client) txcRead(group []pgproto3.FrontendMessage, g *xgroup, t *txTmpl) bool {
	pf := normFormats(g.bind.ParameterFormatCodes, 1)[0]
	rf := normFormats(g.bind.ResultFormatCodes, len(t.fields))
	var key strings.Builder
	key.WriteString(strconv.FormatInt(t.id, 10) + "\x00" + c.txc.beginSQL + "\x00" + strconv.Itoa(int(pf)) + "\x00")
	for _, f := range rf {
		key.WriteByte(byte('0' + f))
	}
	for _, s := range c.txc.sets {
		key.WriteString("\x00" + s.sql + "\x00" + oidsKey(s.oids))
		for _, p := range s.params {
			key.WriteString("\x01" + strconv.Itoa(len(p)) + ":")
			key.Write(p)
		}
	}
	names, wok := c.eng.watched()
	if !wok {
		return c.txcFallback(group)
	}
	var wn []string
	var wv []*string
	for _, n := range names {
		if c.txc.setNames[n] {
			continue // overridden by this transaction's own set_config before the read
		}
		var v *string
		if c.watch != nil {
			v = c.watch[n]
			if _, known := c.watch[n]; !known && c.sess != nil {
				return c.txcFallback(group) // name not covered by the last verification
			}
		}
		wn = append(wn, n)
		wv = append(wv, v)
		key.WriteString("\x03" + n + "=")
		if v == nil {
			key.WriteString("\xff")
		} else {
			key.WriteString(*v)
		}
	}
	b := c.eng.txBatcherFor(key.String(), t, c.txc.beginSQL, c.txc.sets, pf, rf, wn, wv)
	if b.cold() {
		// a shared transaction for a single client costs more than passthrough: replay on own session.
		// The read passed every txc check, so it is state-neutral like the logged prefix.
		stTxBypass.Add(1)
		stTxFallbacks.Add(-1)
		return c.txcFallbackN(group, true)
	}
	c.txcNoteParse(g)
	c.txcLog(group, nil)
	c.drainCancel()
	res, canceled := b.submit(g.bind.Parameters[0], c.cancelCh)
	if canceled {
		c.txc.aborted = true
		c.txc.read = true
		var out []pgproto3.BackendMessage
		if g.parse != nil {
			out = append(out, &pgproto3.ParseComplete{})
		}
		out = append(out, &pgproto3.BindComplete{}, errCanceled)
		return c.reply(out, 'E')
	}
	if res.fallback || res.err != nil {
		// replay this client's transaction on its own session; the read's real result goes to the client
		c.txc.log = c.txc.log[:len(c.txc.log)-1]
		return c.txcFallback(group)
	}
	c.txc.read = true
	var out []pgproto3.BackendMessage
	if g.parse != nil {
		out = append(out, &pgproto3.ParseComplete{})
	}
	out = append(out, &pgproto3.BindComplete{})
	if g.desc != nil {
		out = append(out, rowDesc(t.fields, rf))
	}
	for _, r := range res.rows {
		out = append(out, &pgproto3.DataRow{Values: r})
	}
	out = append(out, &pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(len(res.rows)))})
	stTxCoalesced.Add(1)
	return c.reply(out, 'T')
}

// txcFallback replays everything answered locally in this transaction on the client's own session
// (responses swallowed and checked against what was already sent), then forwards the current group.
func (c *client) txcFallback(group []pgproto3.FrontendMessage) bool {
	return c.txcFallbackN(group, false)
}

// txcFallbackN replays the logged groups (BEGIN, set_config(..., true)/SET LOCAL, Parse-only) on the client's own
// session. Those only affect transaction-local state, so they do not taint the session; groupNeutral says whether
// the final group is also known neutral (a txc-classified read).
func (c *client) txcFallbackN(group []pgproto3.FrontendMessage, groupNeutral bool) bool {
	stTxFallbacks.Add(1)
	if traceOn {
		g := c.parseXGroup(group)
		if g != nil {
			log.Printf("txc fallback at: %.120s", g.sql)
		} else {
			log.Printf("txc fallback (non-statement group, %d msgs)", len(group))
		}
	}
	log := c.txc.log
	c.txc = txState{}
	if !c.ensureSession() {
		return false
	}
	for _, le := range log {
		c.sess.mu.Lock()
		c.sess.swallow = append(c.sess.swallow, swallowSeg{expect: le.expect})
		c.sess.mu.Unlock()
		c.forceNeutral = true
		ok := c.forward(le.group, nil, true)
		c.forceNeutral = false
		if !ok {
			return false
		}
	}
	if group == nil {
		return true
	}
	c.forceNeutral = groupNeutral
	ok := c.forward(group, nil, true)
	c.forceNeutral = false
	return ok
}

func (e *engine) txBatcherFor(k string, t *txTmpl, begin string, sets []setStmt, pfmt int16, rfmts []int16, wn []string, wv []*string) *txBatcher {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.txBatchers == nil {
		e.txBatchers = map[string]*txBatcher{}
	}
	b, ok := e.txBatchers[k]
	if !ok {
		if len(e.txBatchers) > 20000 { // bounded: fingerprints are often unique (per-request claims)
			e.txBatchers = map[string]*txBatcher{}
		}
		b = &txBatcher{e: e, t: t, begin: begin, sets: sets, pfmt: pfmt, rfmts: rfmts, wn: wn, wv: wv}
		e.txBatchers[k] = b
	}
	return b
}

func (b *txBatcher) submit(param []byte, cancel chan struct{}) (txResult, bool) {
	r := &txReq{param: param, t0: time.Now(), done: make(chan txResult, 1)}
	b.mu.Lock()
	b.pending = append(b.pending, r)
	b.arr[b.ai%len(b.arr)] = r.t0
	b.ai++
	b.scheduleLocked()
	b.mu.Unlock()
	select {
	case res := <-r.done:
		return res, false
	case <-cancel:
		stCancelLocal.Add(1)
		return txResult{}, true
	}
}

// cold: nothing pending to join and too few recent arrivals for this fingerprint to expect a batch.
// Records the arrival so a warming fingerprint starts batching.
func (b *txBatcher) cold() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if len(b.pending) > 0 || b.expectedLocked(now) >= cfg.MinExpected {
		return false
	}
	b.arr[b.ai%len(b.arr)] = now
	b.ai++
	return true
}

func (b *txBatcher) expectedLocked(now time.Time) float64 {
	n := b.ai
	if n > len(b.arr) {
		n = len(b.arr)
	}
	if n < 2 {
		return 0
	}
	span := now.Sub(b.arr[(b.ai-n)%len(b.arr)]).Seconds()
	if span <= 0 {
		return 1e9
	}
	return float64(n-1) / span * cfg.Window.Seconds()
}

func (b *txBatcher) scheduleLocked() {
	if len(b.pending) == 0 || b.inflight >= b.e.lanes {
		return
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

func (b *txBatcher) flushLocked() {
	n := len(b.pending)
	if n > cfg.MaxBatch {
		n = cfg.MaxBatch
	}
	batch := append([]*txReq(nil), b.pending[:n]...)
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

var errTxFallback = errors.New("fallback")

func (b *txBatcher) run(batch []*txReq) {
	idx := map[string]int{}
	var us []*uval
	for _, r := range batch {
		k := string(r.param)
		if r.param == nil {
			k = "\xffnull"
		}
		i, ok := idx[k]
		if !ok {
			i = len(us)
			idx[k] = i
			us = append(us, &uval{null: r.param == nil, param: r.param})
		}
		us[i].reqs = append(us[i].reqs, nil)
		_ = i
	}
	stTxBatches.Add(1)
	stTxBatchItems.Add(int64(len(batch)))
	hTxBatchSize.observe(float64(len(batch)))
	c := b.e.getConn()
	ts := time.Now()
	err := b.exec(c, us)
	hBatchExec.observe(time.Since(ts).Seconds())
	b.e.putConn(c)
	for _, r := range batch {
		k := string(r.param)
		if r.param == nil {
			k = "\xffnull"
		}
		u := us[idx[k]]
		if err != nil || u.err != nil {
			r.done <- txResult{fallback: true}
			continue
		}
		r.done <- txResult{rows: u.rows}
	}
}

// exec runs: BEGIN <same>; <same set_config statements, same params>; batched read; COMMIT — one pipeline.
func (b *txBatcher) exec(c *bconn, us []*uval) error {
	// decided before anything is queued on the connection: a text array literal cannot be built safely
	// in this client encoding, so every client replays its transaction on its own session
	if len(us) > 1 && b.pfmt == 0 && !textArraySafe(c.params["client_encoding"]) {
		return errTxFallback
	}
	fe := c.fe
	fe.Send(&pgproto3.Parse{Query: b.begin})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	// the coalescer connection must show the same watched settings the clients' sessions show
	chk := "SELECT 1"
	for _, n := range b.wn {
		chk += ", pg_catalog.current_setting(" + sqlLit(n) + ", true)"
	}
	fe.Send(&pgproto3.Parse{Query: chk})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	for _, s := range b.sets {
		fe.Send(&pgproto3.Parse{Query: s.sql, ParameterOIDs: s.oids})
		fe.Send(&pgproto3.Bind{ParameterFormatCodes: s.pfmts, Parameters: s.params})
		fe.Send(&pgproto3.Execute{})
	}
	single := len(us) == 1
	if single {
		fe.Send(&pgproto3.Parse{Query: b.t.sql, ParameterOIDs: b.t.oids})
		fe.Send(&pgproto3.Bind{ParameterFormatCodes: []int16{b.pfmt}, Parameters: [][]byte{us[0].param}, ResultFormatCodes: b.rfmts})
	} else {
		var p []byte
		if b.pfmt == 0 {
			p = arrayText(us)
		} else {
			p = arrayBinary(us, b.t.paramOID)
		}
		fe.Send(&pgproto3.Parse{Query: b.t.batchSQL})
		fe.Send(&pgproto3.Bind{ParameterFormatCodes: []int16{b.pfmt}, Parameters: [][]byte{p}, ResultFormatCodes: append([]int16{0}, b.rfmts...)})
	}
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Parse{Query: "COMMIT"})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		c.broken = true
		return err
	}
	var failed bool
	var bad error
	var ts byte
	stage := 0 // counts CommandCompletes: BEGIN, check, sets..., read, COMMIT
	readStage := 2 + len(b.sets)
	watchBad := false
	for {
		m, err := fe.Receive()
		if err != nil {
			c.broken = true
			return err
		}
		switch x := m.(type) {
		case *pgproto3.DataRow:
			if stage == 1 {
				if len(x.Values) != 1+len(b.wn) {
					watchBad = true
					continue
				}
				for i, v := range x.Values[1:] {
					w := b.wv[i]
					if (v == nil) != (w == nil) || (v != nil && string(v) != *w) {
						watchBad = true
					}
				}
				continue
			}
			if stage != readStage {
				continue
			}
			if single {
				us[0].rows = append(us[0].rows, copyRow(x.Values))
				continue
			}
			o, e := strconv.Atoi(string(x.Values[0]))
			if e != nil || o < 1 || o > len(us) {
				bad = errors.New("bad ordinal")
				continue
			}
			us[o-1].rows = append(us[o-1].rows, copyRow(x.Values[1:]))
		case *pgproto3.CommandComplete:
			stage++
		case *pgproto3.ErrorResponse:
			failed = true
		case *pgproto3.ReadyForQuery:
			ts = x.TxStatus
		}
		if ts != 0 {
			break
		}
	}
	if ts != 'I' {
		// error inside the transaction: clean it up
		fe.Send(&pgproto3.Query{String: "ROLLBACK"})
		if fe.Flush() != nil || c.readUntilRFQ(func(pgproto3.BackendMessage) {}) != nil {
			c.broken = true
		}
		return errTxFallback
	}
	if watchBad {
		stTxWatchMismatch.Add(1)
		return errTxFallback
	}
	if failed || bad != nil || stage != readStage+2 {
		return errTxFallback
	}
	return nil
}

// ---------- watched placeholder settings ----------

var (
	curSetAnyRe = regexp.MustCompile(`(?i)current_setting\s*\(`)
	curSetLitRe = regexp.MustCompile(`(?i)current_setting\s*\(\s*'([^']+)'`)
	setWriteRe  = regexp.MustCompile(`(?i)set_config\s*\(|\bset\s+(local\s+|session\s+)?[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_.]*\s*(=|to)\b`)
)

// watched returns the placeholder settings that anything a coalesced transaction could execute might
// read: names in current_setting('...') calls in user functions (non-C), RLS policies and views, plus
// PostgREST's response.* settings. Transaction coalescing is disabled for the database (ok=false) if
// any of those reads a setting by a computed name, or if a user function writes settings.
func (e *engine) watched() ([]string, bool) {
	e.watchMu.Lock()
	defer e.watchMu.Unlock()
	ttl := cfg.TemplateTTL
	if ttl <= 0 || ttl > time.Minute {
		ttl = time.Minute
	}
	if e.watchNames != nil && time.Since(e.watchAt) < ttl {
		return e.watchNames, e.watchOK
	}
	b := e.getConn()
	rows, perr, err := b.simpleRows(`SELECT src FROM (
 SELECT p.prosrc AS src FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace
  JOIN pg_catalog.pg_language l ON l.oid OPERATOR(pg_catalog.=) p.prolang
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND l.lanname NOT IN ('c', 'internal')
 UNION ALL SELECT pg_catalog.pg_get_expr(polqual, polrelid) FROM pg_catalog.pg_policy WHERE polqual IS NOT NULL
 UNION ALL SELECT pg_catalog.pg_get_expr(polwithcheck, polrelid) FROM pg_catalog.pg_policy WHERE polwithcheck IS NOT NULL
 UNION ALL SELECT pg_catalog.pg_get_viewdef(c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
  WHERE c.relkind IN ('v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')) s`)
	e.putConn(b)
	if err != nil || perr != nil {
		return []string{}, false
	}
	set := map[string]bool{"response.headers": true, "response.status": true}
	ok := true
	for _, r := range rows {
		src := r[0]
		all := len(curSetAnyRe.FindAllStringIndex(src, -1))
		lits := curSetLitRe.FindAllStringSubmatch(src, -1)
		if all != len(lits) || setWriteRe.MatchString(src) {
			ok = false
		}
		for _, l := range lits {
			if strings.Contains(l[1], ".") {
				set[strings.ToLower(l[1])] = true
			}
		}
	}
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sortStrings(names)
	if len(names) > 64 {
		ok = false
		names = names[:64]
	}
	e.watchNames, e.watchOK, e.watchAt = names, ok, time.Now()
	return names, ok
}

func sortStrings(s []string) { sort.Strings(s) }

// groupMayBegin: cheap pre-filter so non-transaction requests skip txc parsing/regex entirely.
func (c *client) groupMayBegin(group []pgproto3.FrontendMessage) bool {
	for _, m := range group {
		var q string
		switch x := m.(type) {
		case *pgproto3.Parse:
			q = x.Query
		case *pgproto3.Bind:
			c.mu.Lock()
			st := c.stmts[x.PreparedStatement]
			c.mu.Unlock()
			if st == nil {
				continue
			}
			q = st.query
		default:
			continue
		}
		q = strings.TrimLeft(q, " \t\r\n")
		if len(q) >= 5 {
			h := strings.ToLower(q[:5])
			if h == "begin" || h == "start" {
				return true
			}
		}
	}
	return false
}
