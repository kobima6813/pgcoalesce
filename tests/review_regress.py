"""Regression tests for the four bugs found in the pre-launch review (Nov 2026).

  R1  text keys under a non-UTF-8 client_encoding (LATIN1, SJIS) and invalid bytes must give the same
      answer as direct Postgres when they share a batch
  R2  simple-protocol string literal with a backslash while standard_conforming_strings = off
  R3  one client cycling through many session settings must not grow server connections without bound,
      and idle engines must be closed again
  R4  Close followed by a reference to the closed statement/portal inside one message group must return
      the Postgres error and must not take the proxy down

Run against a trust-auth proxy with batching forced, e.g.
  pgcoalesce -config configs-test/correct2ms.ini -set min_expected=0 -set window=20ms \
             -set max_engines=8 -set engine_idle_timeout=3
  python3 tests/review_regress.py <proxy_port> <direct_port> [max_engines] [lanes]
Needs the cq schema from tests/schema.sql and `ALTER ROLE postgres SET search_path = cq, public`.
"""
import asyncio, json, socket, struct, sys, time
import psycopg

PROXY = int(sys.argv[1]) if len(sys.argv) > 1 else 56600
DIRECT = int(sys.argv[2]) if len(sys.argv) > 2 else 55433
MAX_ENGINES = int(sys.argv[3]) if len(sys.argv) > 3 else 8
LANES = int(sys.argv[4]) if len(sys.argv) > 4 else 4
res = {}

# ---------- minimal raw wire client (trust auth) ----------
def msg(t, body=b''): return t + struct.pack('!I', len(body) + 4) + body
def cstr(s): return (s if isinstance(s, bytes) else s.encode()) + b'\0'
def parse(name, sql): return msg(b'P', cstr(name) + cstr(sql) + struct.pack('!H', 0))
def bind(portal, stmt, params):
    b = cstr(portal) + cstr(stmt) + struct.pack('!HH', 0, len(params))
    for p in params:
        b += struct.pack('!i', -1) if p is None else struct.pack('!I', len(p)) + p
    return msg(b'B', b + struct.pack('!H', 0))
def execute(portal): return msg(b'E', cstr(portal) + struct.pack('!I', 0))
def close(kind, name): return msg(b'C', kind + cstr(name))
SYNC = msg(b'S')

class Raw:
    def __init__(self, port, extra=None):
        self.s = socket.create_connection(('127.0.0.1', port), timeout=10)
        p = {'user': 'postgres', 'database': 'postgres'}; p.update(extra or {})
        body = struct.pack('!I', 196608) + b''.join(cstr(k) + cstr(v) for k, v in p.items()) + b'\0'
        self.s.sendall(struct.pack('!I', len(body) + 4) + body); self.buf = b''
        self.ready()
    def one(self):
        while len(self.buf) < 5 or len(self.buf) < 1 + struct.unpack('!I', self.buf[1:5])[0]:
            d = self.s.recv(65536)
            if not d: raise EOFError('connection closed')
            self.buf += d
        n = struct.unpack('!I', self.buf[1:5])[0]
        t, body = self.buf[:1], self.buf[5:1 + n]; self.buf = self.buf[1 + n:]
        return t, body
    def ready(self):
        """read to ReadyForQuery; return a normalised summary: list of ('row', [values]) / ('err', sqlstate) / tag"""
        out = []
        while True:
            t, b = self.one()
            if t == b'Z': return out
            if t == b'D':
                n = struct.unpack('!H', b[:2])[0]; p = 2; vals = []
                for _ in range(n):
                    l = struct.unpack('!i', b[p:p + 4])[0]; p += 4
                    if l < 0: vals.append(None)
                    else: vals.append(b[p:p + l]); p += l
                out.append(('row', vals))
            elif t == b'E':
                f = dict((x[:1], x[1:]) for x in b.split(b'\0') if x)
                out.append(('err', f.get(b'C', b'?').decode()))
            elif t in (b'1', b'2', b'3', b'C'): out.append(t.decode())
    def send(self, *parts): self.s.sendall(b''.join(parts))

su = psycopg.connect(host='127.0.0.1', port=DIRECT, user='postgres', dbname='postgres', autocommit=True)
su.execute("""DROP TABLE IF EXISTS cq.t_enc;
CREATE TABLE cq.t_enc (k text PRIMARY KEY, a text);
INSERT INTO cq.t_enc VALUES ('café','coffee'), ('naïve','simple'), ('zoë','name'), ('plain','ascii'),
 ('ソ','katakana so'), ('表','table'), ('能力','ability'), ('abc','ascii2'),
 (E'a\\\\b', 'one backslash'), (E'a\\\\\\\\b', 'two backslashes')""")

def proxy_alive():
    try:
        c = Raw(PROXY); c.send(msg(b'Q', cstr('SELECT 1'))); c.ready(); return True
    except Exception:
        return False

# ---------- R1: non-UTF-8 client encodings ----------
async def enc_round(port, encoding, keys, rounds=25):
    conns = [await psycopg.AsyncConnection.connect(host='127.0.0.1', port=port, user='postgres', dbname='postgres',
                                                   autocommit=True, client_encoding=encoding) for _ in keys]
    for c in conns: c.prepare_threshold = None
    got = []
    for _ in range(rounds):
        async def one(c, k):
            cur = await c.execute('SELECT a FROM t_enc WHERE k = %s', (k,))
            return await cur.fetchall()
        got.append(await asyncio.gather(*[one(c, k) for c, k in zip(conns, keys)]))
    for c in conns: await c.close()
    return got

def r1():
    out = {}
    for enc, keys in (('LATIN1', ['café', 'naïve', 'zoë', 'plain']), ('SJIS', ['ソ', '表', '能力', 'abc'])):
        d = asyncio.run(enc_round(DIRECT, enc, keys)); p = asyncio.run(enc_round(PROXY, enc, keys))
        wrong = sum(1 for a, b in zip(d, p) for x, y in zip(a, b) if x != y)
        out[enc] = 'ok' if wrong == 0 else 'FAIL: %d of %d answers differ from direct' % (wrong, len(d) * len(keys))
    # invalid UTF-8 next to a valid key in one batch: bad client gets 22021, good client gets its row
    def pair(port):
        a, b = Raw(port), Raw(port)
        q = 'SELECT a FROM t_enc WHERE k = $1'
        r = []
        for _ in range(10):
            a.send(parse('', q), bind('', '', [b'\xff\xfe']), execute(''), SYNC)
            b.send(parse('', q), bind('', '', [b'plain']), execute(''), SYNC)
            r.append((a.ready(), b.ready()))
        return r
    # compare rows and SQLSTATEs only: Postgres rejects a bad value at Bind, the proxy reports the same
    # error at Execute (it answers Bind itself), so the BindComplete marker legitimately differs
    strip = lambda rs: [[[m for m in side if isinstance(m, tuple)] for side in r] for r in rs]
    d, p = strip(pair(DIRECT)), strip(pair(PROXY))
    out['invalid_bytes'] = 'ok' if d == p else 'FAIL: %r vs direct %r' % (p[0], d[0])
    return out

# ---------- R2: backslash literal with standard_conforming_strings = off ----------
def r2():
    def ask(port):
        c = psycopg.connect(host='127.0.0.1', port=port, user='postgres', dbname='postgres', autocommit=True)
        c.execute('SET standard_conforming_strings = off'); c.execute('SET escape_string_warning = off')
        r = [c.execute(r"SELECT a FROM t_enc WHERE k = 'a\\b'").fetchall() for _ in range(3)]
        c.execute('RESET standard_conforming_strings')
        r += [c.execute(r"SELECT a FROM t_enc WHERE k = 'a\\b'").fetchall() for _ in range(3)]
        c.close(); return r
    d, p = ask(DIRECT), ask(PROXY)
    return 'ok' if d == p else 'FAIL: proxy %r, direct %r' % (p, d)

# ---------- R3: session-setting churn must not leak server connections ----------
def r3():
    def nconn(): return su.execute("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()").fetchone()[0]
    time.sleep(5); base = nconn(); peak = base; bad = 0   # let engines of the earlier tests go idle and close
    c = psycopg.connect(host='127.0.0.1', port=PROXY, user='postgres', dbname='postgres', autocommit=True)
    for i in range(80):
        c.execute('SET search_path = tenant_%d, cq, public' % i)
        if c.execute('SELECT a FROM t_enc WHERE k = %s', ('plain',)).fetchone() != ('ascii',): bad += 1
        if i % 5 == 4: peak = max(peak, nconn())
    peak = max(peak, nconn())
    c.close()
    limit = (MAX_ENGINES + 2) * LANES + 4          # engines being closed may overlap briefly; +own session
    time.sleep(8); after = nconn()
    ok = bad == 0 and peak - base <= limit and after - base <= LANES + 1
    return {'result': 'ok' if ok else 'FAIL', 'wrong_answers': bad, 'server_conns_base': base, 'peak_growth': peak - base,
            'allowed_growth': limit, 'left_after_idle': after - base}

# ---------- R4: Close then reference inside one group ----------
def r4():
    q = 'SELECT a FROM t_enc WHERE k = $1'
    seqs = {
        'close_portal_then_execute': [parse('', q), bind('p', '', [b'plain']), close(b'P', 'p'), execute('p'), SYNC],
        'close_stmt_then_bind': [parse('s1', q), close(b'S', 's1'), bind('', 's1', [b'plain']), execute(''), SYNC],
        'close_stmt_then_describe': [parse('s2', q), close(b'S', 's2'), msg(b'D', b'S' + cstr('s2')), SYNC],
        'bind_closed_stmt_next_group': [bind('', 's1', [b'plain']), execute(''), SYNC],   # s1 was closed above
        'close_then_reparse_ok': [parse('s3', q), close(b'S', 's3'), parse('s3', q), bind('', 's3', [b'plain']), execute(''), SYNC],
    }
    out = {}
    try:
        d, p = Raw(DIRECT), Raw(PROXY)
        for name, parts in seqs.items():
            d.send(*parts); want = d.ready()
            try:
                p.send(*parts); got = p.ready()
            except Exception as e:
                got = 'connection lost (%s)' % type(e).__name__
            out[name] = 'ok' if got == want else 'FAIL: proxy %r, direct %r' % (got, want)
    except Exception as e:
        out['error'] = repr(e)
    time.sleep(0.5)
    out['proxy_still_running'] = 'ok' if proxy_alive() else 'FAIL: proxy process is gone'
    return out

for name, fn in (('R1_encodings', r1), ('R2_backslash_literal', r2), ('R3_engine_churn', r3), ('R4_close_in_group', r4)):
    try:
        res[name] = fn()
    except Exception as e:
        res[name] = 'FAIL: %s: %s' % (type(e).__name__, str(e).strip()[:200])
fails = json.dumps(res).count('FAIL')
res['failed_checks'] = fails
print(json.dumps(res, indent=1, ensure_ascii=False))
sys.exit(1 if fails else 0)
