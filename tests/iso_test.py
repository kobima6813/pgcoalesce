"""Adversarial isolation suite for pgcoalesce.
Personas with different roles / SET ROLE / search_path / RLS tenant / formatting GUCs / temp tables /
state changed inside functions hammer the proxy concurrently on overlapping keys. Every result is
compared with the result of the same persona on a direct Postgres connection. Zero mismatches required.
Passwords are read from 600 files and never printed."""
import asyncio, random, sys, time, json, urllib.request
import asyncpg, psycopg

SEC = '/workspace/coalesce/secrets/'
pw = {u: open(SEC + u + '.pw').read().strip() for u in ('app_a', 'app_b', 'app_md5')}
PROXY = dict(host='127.0.0.1', port=int(sys.argv[3]) if len(sys.argv) > 3 else 56610)
DIRECT = dict(host='127.0.0.1', port=55433)
STATS = sys.argv[4] if len(sys.argv) > 4 else 'http://127.0.0.1:56710/stats'

Q = {
 'items': 'SELECT id, v FROM items WHERE id = $1',
 'secret': 'SELECT id, s FROM secret WHERE id = $1',
 'docs': 'SELECT id, tenant, body FROM docs WHERE id = $1',
 'pub': 'SELECT id, ts, d, iv, f, b, m FROM pub WHERE id = $1',
 'sb': 'SELECT id, f FROM v_sb WHERE id = $1',
 'forupd': 'SELECT id, v FROM items WHERE id = $1 FOR UPDATE',
}
FMT = ["SET TimeZone = 'America/St_Johns'", "SET DateStyle = 'SQL, DMY'", "SET IntervalStyle = 'sql_standard'",
       "SET extra_float_digits = 0", "SET bytea_output = 'escape'", "SET lc_monetary = 'C'"]
# persona: (user, startup options, setup statements, queries)
P = {
 'A0_plain':      ('app_a', None, [], ['items', 'secret', 'pub', 'sb', 'forupd']),
 'A1_searchpath': ('app_a', None, ["SET search_path = s2, iso"], ['items', 'secret']),
 'A2_tenant1':    ('app_a', None, ["SELECT set_config('app.tenant_id', '1', false)"], ['docs', 'items']),
 'A2_tenant2':    ('app_a', None, ["SELECT set_config('app.tenant_id', '2', false)"], ['docs', 'items']),
 'A3_setrole_b':  ('app_a', None, ["SET ROLE app_b"], ['secret', 'items', 'docs']),
 'A4_formats':    ('app_a', None, FMT, ['pub']),
 'A5_temp':       ('app_a', None, ["CREATE TEMP TABLE items(id int PRIMARY KEY, v text)", "INSERT INTO items SELECT g, 'tmp-'||g FROM generate_series(1,500) g"], ['items']),
 'A6_fn_path':    ('app_a', None, ["SELECT iso.sneaky_path()"], ['items', 'secret']),
 'A7_fn_tz':      ('app_a', None, ["SELECT iso.sneaky_tz()"], ['pub']),
 'A8_comment_set':('app_a', None, ["/* hidden */ SET search_path = s2, iso"], ['items']),
 'B0_plain':      ('app_b', None, [], ['items', 'secret', 'pub']),
 'B1_tenant2':    ('app_b', None, ["SELECT set_config('app.tenant_id', '2', false)"], ['docs']),
 'M0_md5':        ('app_md5', None, [], ['items', 'secret']),
 'O0_options':    ('app_a', '-c search_path=s2,iso', [], ['items']),
 'O1_opt_tz':     ('app_a', '-c TimeZone=Pacific/Chatham -c DateStyle=German', [], ['pub']),
}
TOGGLE = ("SET search_path = s2, iso", "RESET search_path")  # toggler persona flips between states

def key(): # heavy overlap so different personas land in the same batches
    return min(int(random.paretovariate(1.2)), 60)

def norm_err(e):
    return ('ERR', getattr(e, 'sqlstate', None) or type(e).__name__)

async def pg_conn(kind, user, opts, target):
    kw = dict(target, user=user, password=pw[user], dbname='postgres')
    if kind == 'apg':
        sk = {'server_settings': {}}
        if opts:
            sk['server_settings']['options'] = opts
        return await asyncpg.connect(host=kw['host'], port=kw['port'], user=user, password=pw[user], database='postgres', ssl='require', statement_cache_size=100, **sk)
    c = await psycopg.AsyncConnection.connect(host=kw['host'], port=kw['port'], user=user, password=pw[user], dbname='postgres', sslmode='require', options=opts or '', autocommit=True, prepare_threshold=0 if kind == 'pgp' else None)
    return c

async def run_q(kind, c, qname, k):
    sql = Q[qname]
    try:
        if kind == 'apg':
            rows = await c.fetch(sql, k)
            return ('OK', [tuple(r) for r in rows])
        cur = c.cursor()
        await cur.execute(sql.replace('$1', '%s'), (k,))
        res = cur.pgresult
        return ('OK', [tuple(res.get_value(i, j) for j in range(res.nfields)) for i in range(res.ntuples)])
    except Exception as e:
        return norm_err(e)

async def setup(kind, c, stmts):
    for s in stmts:
        if kind == 'apg':
            await c.execute(s)
        else:
            await c.execute(s)

async def expected_table(kinds):
    exp = {}
    for kind in kinds:
        for name, (user, opts, st, qs) in P.items():
            c = await pg_conn(kind, user, opts, DIRECT)
            await setup(kind, c, st)
            for q in qs:
                for k in range(1, 61):
                    exp[(kind, name, q, k)] = await run_q(kind, c, q, k)
            await c.close()
        for state in (0, 1):
            c = await pg_conn(kind, 'app_a', None, DIRECT)
            if state == 1:
                await setup(kind, c, [TOGGLE[0]])
            for k in range(1, 61):
                exp[(kind, 'T_toggle%d' % state, 'items', k)] = await run_q(kind, c, 'items', k)
            await c.close()
    return exp

stats = {'ops': 0, 'mismatch': 0, 'examples': [], 'per': {}}

def check(exp, kind, name, q, k, got):
    stats['ops'] += 1
    stats['per'][name] = stats['per'].get(name, 0) + 1
    want = exp[(kind, name, q, k)]
    if got != want:
        stats['mismatch'] += 1
        if len(stats['examples']) < 10:
            stats['examples'].append({'persona': name, 'kind': kind, 'q': q, 'k': k, 'got': repr(got)[:200], 'want': repr(want)[:200]})

async def worker(exp, kind, name, deadline):
    user, opts, st, qs = P[name]
    c = await pg_conn(kind, user, opts, PROXY)
    await setup(kind, c, st)
    while time.time() < deadline:
        q = random.choice(qs); k = key()
        check(exp, kind, name, q, k, await run_q(kind, c, q, k))
    await c.close()

async def toggler(exp, kind, deadline):
    c = await pg_conn(kind, 'app_a', None, PROXY)
    state, n = 0, 0
    while time.time() < deadline:
        n += 1
        if n % 7 == 0:
            state ^= 1
            await setup(kind, c, [TOGGLE[0] if state else TOGGLE[1]])
        k = key()
        check(exp, kind, 'T_toggle%d' % state, 'items', k, await run_q(kind, c, 'items', k))
    await c.close()

async def bump_check():
    """volatile attribute-notation function must run exactly once per call (never deduplicated)"""
    c = await pg_conn('apg', 'app_a', None, PROXY)
    d = await pg_conn('apg', 'app_a', None, DIRECT)
    before = await d.fetchval("SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM iso.bump_seq")
    async def one():
        cc = await pg_conn('apg', 'app_a', None, PROXY)
        for _ in range(50):
            await cc.fetch("SELECT p.bump FROM pub p WHERE id = $1", 5)
        await cc.close()
    await asyncio.gather(*[one() for _ in range(20)])
    after = await d.fetchval("SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM iso.bump_seq")
    await c.close(); await d.close()
    return int(after) - int(before)

async def main():
    dur = float(sys.argv[1]) if len(sys.argv) > 1 else 30
    random.seed(int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    kinds = ['apg', 'pgp']
    t0 = time.time()
    exp = await expected_table(kinds)
    s0 = json.load(urllib.request.urlopen(STATS))
    deadline = time.time() + dur
    tasks = []
    for kind in kinds:
        for name in P:
            for _ in range(3):
                tasks.append(worker(exp, kind, name, deadline))
        for _ in range(3):
            tasks.append(toggler(exp, kind, deadline))
        for _ in range(12):  # extra plain clients so batches are big and mixed
            tasks.append(worker(exp, kind, 'A0_plain', deadline))
    await asyncio.gather(*tasks)
    bumps = await bump_check()
    s1 = json.load(urllib.request.urlopen(STATS))
    delta = {k: s1[k] - s0.get(k, 0) for k in s1 if isinstance(s1[k], int)}
    out = {'ops': stats['ops'], 'mismatches': stats['mismatch'], 'examples': stats['examples'],
           'volatile_calls_expected': 1000, 'volatile_calls_seen': bumps,
           'proxy_delta': {k: delta[k] for k in ('coalesced_reqs', 'batches', 'unique', 'verifies', 'verify_mismatch', 'forward_groups', 'tmpl_ok', 'tmpl_no')},
           'personas': stats['per'], 'secs': round(time.time() - t0, 1)}
    print(json.dumps(out, indent=1))

asyncio.run(main())
