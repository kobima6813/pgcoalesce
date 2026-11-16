"""Live check for batch_timeout (issue #1).

Start a proxy with a long window so lookups are merged, and a short batch_timeout, e.g.:
  pgcoalesce -config conf.ini -set window=15ms -set min_expected=0 -set batch_timeout=200ms
then: python3 batch_timeout_test.py <proxy_port> <direct_port> <metrics_url> [psql]

A second session holds ACCESS EXCLUSIVE on the table, so every merged batch blocks. The proxy must
cancel the batch after batch_timeout, run every request individually on its client's own session,
and every client must get exactly its own row once the lock is released. Then normal traffic must
keep flowing on the replaced coalescer connections with no mismatches.
"""
import asyncio, json, sys, time, urllib.request
import asyncpg

PROXY, DIRECT, METRICS = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
DSN = "postgresql://postgres@127.0.0.1:%d/postgres?sslmode=disable"
SQL = "SELECT id, v FROM public.bt_lock WHERE id = $1"

def stats():
    return json.load(urllib.request.urlopen(METRICS))

async def ext_client(pool, k):
    async with pool.acquire() as c:
        t0 = time.monotonic()
        r = await c.fetch(SQL, k)
        return k, [tuple(x) for x in r], time.monotonic() - t0

PSQL = sys.argv[4] if len(sys.argv) > 4 else "psql"

async def simple_client(k):
    # psql uses the simple query protocol
    t0 = time.monotonic()
    p = await asyncio.create_subprocess_exec(PSQL, "-h", "127.0.0.1", "-p", str(PROXY), "-U", "postgres", "-d", "postgres",
        "-AtF", "|", "-c", "SELECT id, v FROM public.bt_lock WHERE id = %d" % k, stdout=asyncio.subprocess.PIPE)
    out, _ = await p.communicate()
    rows = [(int(a), b) for a, b in (l.split("|") for l in out.decode().splitlines() if l)]
    return k, rows, time.monotonic() - t0

async def main():
    fails = 0
    pool = await asyncpg.create_pool(DSN % PROXY, min_size=40, max_size=40, statement_cache_size=0)
    for k in range(1, 41):  # warm: classify the template
        await pool.fetch(SQL, k)
    s0 = stats()
    lock = await asyncpg.connect(DSN % DIRECT)
    await lock.execute("BEGIN; LOCK TABLE public.bt_lock IN ACCESS EXCLUSIVE MODE")
    hold = 1.2
    tasks = [ext_client(pool, k) for k in range(100, 140)]
    tasks += [simple_client(k) for k in range(200, 220)]
    fut = asyncio.gather(*tasks)
    await asyncio.sleep(hold)
    await lock.execute("COMMIT")
    res = await fut
    s1 = stats()
    for k, rows, el in res:
        if rows != [(k, "v%d" % k)]:
            fails += 1
            print("MISMATCH", k, rows)
    lat = sorted(el for _, _, el in res)
    d = {x: s1[x] - s0[x] for x in ("batch_timeouts", "batch_retries", "batches", "fallbacks")}
    print("locked phase: clients=%d mismatches=%d latency min=%.3fs max=%.3fs (lock held %.1fs)" % (len(res), fails, lat[0], lat[-1], hold), d)
    if d["batch_timeouts"] < 1 or d["batch_retries"] < 1:
        fails += 1
        print("FAIL: no batch timed out")
    # normal traffic afterwards: replaced connections, no timeouts, exact rows
    s0 = stats()
    async def one(k):
        r = await pool.fetch(SQL, k)
        return k, [tuple(x) for x in r]
    n = 0
    for rnd in range(50):
        out = await asyncio.gather(*[one((rnd * 37 + i) % 1000 + 1) for i in range(40)])
        for k, rows in out:
            n += 1
            if rows != [(k, "v%d" % k)]:
                fails += 1
                print("MISMATCH after", k, rows)
    s1 = stats()
    d = {x: s1[x] - s0[x] for x in ("batch_timeouts", "batch_retries", "batches", "fallbacks")}
    print("after: ops=%d" % n, d)
    if d["batch_timeouts"] != 0:
        fails += 1
        print("FAIL: timeouts in normal traffic")
    await pool.close(); await lock.close()
    print("RESULT", "OK" if fails == 0 else "FAIL %d" % fails)
    sys.exit(1 if fails else 0)

asyncio.run(main())
