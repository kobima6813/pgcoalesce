package main

import (
	"regexp"
	"strconv"
	"strings"
)

// Strict matcher for "SELECT <ident list> FROM <table> [[AS] alias] WHERE <key> = <$1|literal>".
// Anything else (ORDER BY, LIMIT, AND, casts, functions, comments, FOR UPDATE, ...) does not match → passthrough.

const identRe = `(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+")`
const qidentRe = identRe + `(?:\.` + identRe + `)?`

var (
	stmtRe = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+(` + qidentRe + `)(?:\s+(?:AS\s+)?(` + identRe + `))?\s+WHERE\s+(` + qidentRe + `)\s*=\s*(\$1|-?[0-9]+|'(?:[^']|'')*')\s*;?\s*$`)
	itemRe = regexp.MustCompile(`(?is)^\s*(?:\*|` + identRe + `\.\*|` + qidentRe + `(?:\s+(?:AS\s+)?` + identRe + `)?)\s*$`)
	qualRe = regexp.MustCompile(`(?is)^\s*(` + identRe + `)\.(?:\*|` + identRe + `)`)
)

var keywords = map[string]bool{"where": true, "limit": true, "order": true, "group": true, "join": true, "inner": true, "left": true, "right": true,
	"full": true, "cross": true, "natural": true, "on": true, "using": true, "tablesample": true, "only": true, "lateral": true, "from": true,
	"select": true, "for": true, "offset": true, "having": true, "union": true, "window": true, "fetch": true, "as": true, "distinct": true, "all": true}

type spec struct {
	list    []string
	table   string // as written
	alias   string // as written, "" if none
	keyLast string // key column as written (last part)
	keyName string // normalized column name for catalog lookup
	qual    string // qualifier used in rewrite
	tmplSQL string // query with literal replaced by $1 (or original)
	litKind byte   // 0=$1, 'i' integer literal, 's' string literal
	litText string // decoded literal
	litInt  int64
}

func normIdent(s string) string {
	if strings.HasPrefix(s, `"`) {
		return strings.Trim(s, `"`)
	}
	return strings.ToLower(s)
}

// split "a.b" respecting quotes (identRe never contains '.' inside quotes we accept? quoted may) — keep simple
func splitQ(s string) []string {
	var out []string
	cur := ""
	inq := false
	for _, r := range s {
		if r == '"' {
			inq = !inq
		}
		if r == '.' && !inq {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, cur)
}

func splitList(s string) []string {
	var out []string
	cur := ""
	inq := false
	for _, r := range s {
		if r == '"' {
			inq = !inq
		}
		if r == ',' && !inq {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, cur)
}

func matchSQL(q string) *spec {
	if len(q) > 4000 || strings.Contains(q, "--") || strings.Contains(q, "/*") {
		return nil
	}
	m := stmtRe.FindStringSubmatchIndex(q)
	if m == nil {
		return nil
	}
	g := func(i int) string {
		if m[2*i] < 0 {
			return ""
		}
		return q[m[2*i]:m[2*i+1]]
	}
	sp := &spec{table: g(2), alias: g(3)}
	tparts := splitQ(sp.table)
	for _, p := range tparts {
		if keywords[strings.ToLower(p)] {
			return nil
		}
	}
	if sp.alias != "" && keywords[strings.ToLower(sp.alias)] {
		return nil
	}
	sp.qual = sp.alias
	tlast := tparts[len(tparts)-1]
	if sp.qual == "" {
		sp.qual = sp.table
	}
	allowedQual := func(x string) bool {
		if sp.alias != "" {
			return normIdent(x) == normIdent(sp.alias)
		}
		return normIdent(x) == normIdent(tlast)
	}
	// select list
	for _, it := range splitList(g(1)) {
		if !itemRe.MatchString(it) {
			return nil
		}
		words := strings.Fields(it)
		for _, w := range words {
			lw := strings.ToLower(w)
			if lw != "as" && keywords[lw] {
				return nil
			}
		}
		if qm := qualRe.FindStringSubmatch(it); qm != nil && !allowedQual(qm[1]) {
			return nil
		}
		sp.list = append(sp.list, strings.TrimSpace(it))
	}
	kp := splitQ(g(4))
	if len(kp) == 2 && !allowedQual(kp[0]) {
		return nil
	}
	sp.keyLast = kp[len(kp)-1]
	if keywords[strings.ToLower(sp.keyLast)] {
		return nil
	}
	sp.keyName = normIdent(sp.keyLast)
	lit := g(5)
	sp.tmplSQL = q
	switch {
	case lit == "$1":
	case strings.HasPrefix(lit, "'"):
		sp.litKind = 's'
		sp.litText = strings.ReplaceAll(lit[1:len(lit)-1], "''", "'")
	default:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			return nil
		}
		sp.litKind = 'i'
		sp.litInt = v
		sp.litText = strconv.FormatInt(v, 10)
	}
	if sp.litKind != 0 {
		sp.tmplSQL = q[:m[10]] + "$1" + q[m[11]:]
	}
	return sp
}

// rewritePos: ANY() index scan; routing column = positions of the row's key in the param array (handles '7' vs '007', bpchar padding, ...)
func (sp *spec) rewritePos(typ string) string {
	items := make([]string, len(sp.list))
	for i, it := range sp.list {
		if it == "*" {
			items[i] = sp.qual + ".*"
		} else {
			items[i] = it
		}
	}
	as := ""
	if sp.alias != "" {
		as = " AS " + sp.alias
	}
	k := sp.qual + "." + sp.keyLast
	return "SELECT array_positions($1::" + typ + "[], " + k + "), " + strings.Join(items, ", ") + " FROM " + sp.table + as + " WHERE " + k + " = ANY($1::" + typ + "[])"
}

func (sp *spec) rewrite(typ string) string {
	items := make([]string, len(sp.list))
	for i, it := range sp.list {
		if it == "*" {
			items[i] = sp.qual + ".*"
		} else {
			items[i] = it
		}
	}
	as := ""
	if sp.alias != "" {
		as = " AS " + sp.alias
	}
	return "SELECT __cq.__cq_o, " + strings.Join(items, ", ") + " FROM unnest($1::" + typ + "[]) WITH ORDINALITY AS __cq(__cq_v, __cq_o) JOIN " +
		sp.table + as + " ON " + sp.qual + "." + sp.keyLast + " = __cq.__cq_v"
}
