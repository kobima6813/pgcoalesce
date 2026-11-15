"""Randomized differential test: proxy vs direct Postgres, many drivers/modes, under concurrency."""
import asyncio, random, uuid, json, sys, time, urllib.request
import asyncpg, psycopg
from psycopg import AsyncClientCursor

PROXY = dict(host='127.0.0.1', port=56600, user='postgres', database='postgres')
DIRECT = dict(host='127.0.0.1', port=55433, user='postgres', database='postgres')
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
DURATION = float(sys.argv[2]) if len(sys.argv) > 2 else 60
rnd = random.Random(SEED)

def k_int(r):
    c = r.random()
    if c < 0.05: return None
    if c < 0.15: return r.choice([-2147483648, 2147483647, 0, -1, 999999])
    if c < 0.45: return r.randint(1, 20)          # hot set → duplicates
    return r.randint(-60, 5100)
def k_big(r):
    c = r.random()
    if c < 0.05: return None
    if c < 0.15: return r.choice([9223372036854775807, -9223372036854775808, 0, 5])
    return r.randint(1, 3100) * 1000003 * 1000
def k_uuid(r):
    c = r.random()
    if c < 0.05: return None
    if c < 0.2: return uuid.uuid4()
    import hashlib
    return uuid.UUID(hashlib.md5(('u%d' % r.randint(1, 3100)).encode()).hexdigest())
SPECIAL = ['', 'NULL', 'a"b', 'back\\slash', 'c,o{m}m,a', '  spaced  ', 'ünïcødé ✓', 'null', "'single'", 'missing"\\{,}', 'KEY1']
def k_text(r):
    c = r.random()
    if c < 0.05: return None
    if c < 0.35: return r.choice(SPECIAL)
    return 'key%d' % r.randint(1, 3100)
def k_vc(r): return None if r.random() < .05 else 'vc%d' % r.randint(1, 520)
def k_ch(r):
    if r.random() < .05: return None
    s = 'c%d' % r.randint(1, 520)
    return s + ' ' * r.randint(0, 5 - len(s)) if len(s) < 5 and r.random() < .5 else s
def k_small(r): return r.randint(0, 120)

SHAPES = [
    ("SELECT * FROM t_int WHERE id = $1", k_int, True),
    ("SELECT a, id, c FROM t_int WHERE id = $1", k_int, True),
    ("SELECT t.* FROM t_int t WHERE t.id = $1", k_int, True),
    ("SELECT x.b AS bee, x.id FROM cq.t_int AS x WHERE x.id = $1", k_int, True),
    ("select id, i, f, e, h, g, d from t_int where id = $1;", k_int, True),
    ("SELECT * FROM t_int WHERE id = $1 ORDER BY id LIMIT 1", k_int, False),
    ("SELECT * FROM t_int WHERE id = $1 AND b > 0", k_int, False),
    ("SELECT id, a FROM t_int WHERE id = $1 FOR SHARE", k_int, False),
    ("SELECT * FROM t_big WHERE id = $1", k_big, True),
    ("SELECT a, id FROM t_uuid WHERE id = $1", k_uuid, True),
    ("SELECT * FROM t_text WHERE k = $1", k_text, True),
    ("SELECT id, a FROM t_text tt WHERE tt.k = $1", k_text, True),
    ("SELECT * FROM t_vc WHERE k = $1", k_vc, True),
    ("SELECT * FROM t_ch WHERE k = $1", k_ch, True),
    ("SELECT * FROM t_nouniq WHERE k = $1", k_small, False),
    ("SELECT * FROM v_int WHERE id = $1", k_int, False),
    ("SELECT * FROM t_rls WHERE id = $1", k_small, False),
    ('SELECT "Val", v, o FROM "Mixed Case" WHERE "Id" = $1', k_small, True),
    ('SELECT * FROM "Mixed Case" m WHERE m."Id" = $1', k_small, True),
]
# shapes whose param can't be bound by type-strict drivers for some keys
def psy(sql): return sql.replace('$1', '%s')

stats = {'ops': 0, 'mismatch': 0, 'errors_same': 0, 'by_mode': {}}
mismatches = []

def norm_rows(rows, unordered):
    out = [tuple(r) for r in rows]
    return sorted(out, key=repr) if unordered else out

def record(mode, sql, key, a, b):
    stats['ops'] += 1
    stats['by_mode'][mode] = stats['by_mode'].get(mode, 0) + 1
    if a != b:
        stats['mismatch'] += 1
        if len(mismatches) < 20:
            mismatches.append({'mode': mode, 'sql': sql, 'key': repr(key), 'proxy': repr(a)[:300], 'direct': repr(b)[:300]})
    elif a and a[0] == 'ERR':
        stats['errors_same'] += 1

async def run_asyncpg(pc, dc, sql, key, unordered):
    async def one(c):
        try:
            rs = await c.fetch(sql, key)
            return ('OK', list(rs[0].keys()) if rs else None, norm_rows(rs, unordered))
        except asyncpg.PostgresError as e:
            return ('ERR', e.sqlstate)
        except (asyncpg.exceptions.DataError, OverflowError, TypeError, ValueError) as e:
            return ('CLIENTERR', type(e).__name__)
    return await one(pc), await one(dc)

async def run_psy(pc, dc, sql, key, unordered, binary, prepare):
    async def one(c):
        try:
            cur = await c.execute(psy(sql), (key,), binary=binary, prepare=prepare)
            rs = await cur.fetchall()
            return ('OK', [d.name for d in cur.description] if rs else None, norm_rows(rs, unordered))
        except psycopg.errors.Error as e:
            return ('ERR', e.sqlstate)
    return await one(pc), await one(dc)

async def run_psy_literal(pc, dc, sql, key, unordered):  # client-side binding → simple Query protocol with literals
    async def one(c):
        try:
            cur = AsyncClientCursor(c)
            await cur.execute(psy(sql), (key,))
            rs = await cur.fetchall()
            return ('OK', [d.name for d in cur.description] if rs else None, norm_rows(rs, unordered))
        except psycopg.errors.Error as e:
            return ('ERR', e.sqlstate)
    return await one(pc), await one(dc)

async def run_psy_pipeline(pc, dc, items):
    async def one(c):
        out = []
        try:
            async with c.pipeline():
                curs = []
                for sql, key, unordered in items:
                    curs.append((await c.execute(psy(sql), (key,)), unordered))
            for cur, unordered in curs:
                out.append(norm_rows(await cur.fetchall(), unordered))
            return ('OK', out)
        except psycopg.errors.Error as e:
            return ('ERR', e.sqlstate)
    return await one(pc), await one(dc)

async def worker(wid, deadline):
    r = random.Random(SEED * 1000 + wid)
    kind = wid % 5
    if kind == 0:
        pc = await asyncpg.connect(**PROXY); dc = await asyncpg.connect(**DIRECT)
    else:
        pc = await psycopg.AsyncConnection.connect(host='127.0.0.1', port=56600, user='postgres', dbname='postgres', autocommit=True)
        dc = await psycopg.AsyncConnection.connect(host='127.0.0.1', port=55433, user='postgres', dbname='postgres', autocommit=True)
        pc.prepare_threshold = dc.prepare_threshold = r.choice([0, 2, 5, None])
    while time.time() < deadline:
        sql, kg, _ = r.choice(SHAPES)
        key = kg(r)
        unordered = 'nouniq' in sql
        if kind == 0:
            a, b = await run_asyncpg(pc, dc, sql, key, unordered); mode = 'asyncpg-binary-named'
        elif kind == 1:
            a, b = await run_psy(pc, dc, sql, key, unordered, False, None); mode = 'psycopg-text'
        elif kind == 2:
            a, b = await run_psy(pc, dc, sql, key, unordered, True, None); mode = 'psycopg-binary'
        elif kind == 3:
            a, b = await run_psy_literal(pc, dc, sql, key, unordered); mode = 'psycopg-simple-literal'
        else:
            items = [(lambda s: (s[0], s[1](r), 'nouniq' in s[0]))(r.choice(SHAPES)) for _ in range(r.randint(2, 6))]
            a, b = await run_psy_pipeline(pc, dc, items); mode = 'psycopg-pipeline'; sql = ' | '.join(i[0] for i in items); key = [i[1] for i in items]
        record(mode, sql, key, a, b)
    await pc.close(); await dc.close()

# --- transactional / read-your-writes / taint checks ---
txstats = {'ryw_ok': 0, 'ryw_fail': 0, 'txn_own_write_ok': 0, 'txn_own_write_fail': 0, 'rr_snapshot_ok': 0, 'rr_snapshot_fail': 0, 'taint_ok': 0, 'taint_fail': 0}
async def tx_worker(wid, deadline):
    r = random.Random(SEED * 7 + wid)
    pc = await asyncpg.connect(**PROXY); dc = await asyncpg.connect(**DIRECT)
    ids = list(range(wid * 10 + 1, wid * 10 + 11))
    q = 'SELECT val FROM t_rw WHERE id = $1'
    while time.time() < deadline:
        rid = r.choice(ids); v = r.randint(1, 10**6)
        await pc.execute('UPDATE t_rw SET val = $1 WHERE id = $2', v, rid)
        got = await pc.fetchval(q, rid)
        txstats['ryw_ok' if got == v else 'ryw_fail'] += 1
        tr = pc.transaction(); await tr.start()
        await pc.execute('UPDATE t_rw SET val = $1 WHERE id = $2', v + 1, rid)
        got = await pc.fetchval(q, rid)
        txstats['txn_own_write_ok' if got == v + 1 else 'txn_own_write_fail'] += 1
        await tr.rollback()
        tr = pc.transaction(isolation='repeatable_read'); await tr.start()
        first = await pc.fetchval(q, rid)
        await dc.execute('UPDATE t_rw SET val = $1 WHERE id = $2', v + 2, rid)
        second = await pc.fetchval(q, rid)
        await tr.commit()
        after = await pc.fetchval(q, rid)
        ok = first == v and second == v and after == v + 2
        txstats['rr_snapshot_ok' if ok else 'rr_snapshot_fail'] += 1
    await pc.close(); await dc.close()

async def taint_check():
    pc = await asyncpg.connect(**PROXY)
    await pc.fetchval('SELECT a FROM t_int WHERE id = $1', 5)  # coalescable, resolves to cq.t_int
    await pc.execute('SET search_path = public')
    try:
        await pc.fetchval('SELECT a FROM t_int WHERE id = $1', 5)
        txstats['taint_fail'] += 1   # must fail: t_int not in public
    except asyncpg.UndefinedTableError:
        txstats['taint_ok'] += 1
    await pc.execute('CREATE TEMP TABLE users (id int primary key, email text)')
    await pc.execute("INSERT INTO pg_temp.users VALUES (42, 'temp-shadow')")
    await pc.execute('SET search_path = pg_temp, public')
    v = await pc.fetchval('SELECT email FROM users WHERE id = $1', 42)
    txstats['taint_ok' if v == 'temp-shadow' else 'taint_fail'] += 1
    await pc.close()

def proxy_stats():
    return json.loads(urllib.request.urlopen('http://127.0.0.1:56700/stats').read())

async def main():
    s0 = proxy_stats()
    deadline = time.time() + DURATION
    await taint_check()
    await asyncio.gather(*[worker(i, deadline) for i in range(60)], *[tx_worker(i, deadline) for i in range(20)])
    s1 = proxy_stats()
    d = {k: s1[k] - s0[k] for k in s1 if k != 'queue_max'}
    d['avg_batch'] = round(d['batch_items'] / max(1, d['batches']), 2)
    d['multi_key_batches'] = d['batches'] - d['singles']
    print(json.dumps({'seed': SEED, 'diff': stats, 'txn': txstats, 'proxy_delta': d, 'mismatch_examples': mismatches}, indent=1, default=str))

asyncio.run(main())
