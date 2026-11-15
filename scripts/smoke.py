import asyncio, asyncpg, psycopg
async def a():
    c = await asyncpg.connect(host='127.0.0.1', port=56600, user='postgres', database='postgres', statement_cache_size=100)
    print('asyncpg', await c.fetchrow('SELECT * FROM users WHERE id = $1', 42))
    print('asyncpg missing', await c.fetchrow('SELECT * FROM users WHERE id = $1', 4242424))
    rows = await asyncio.gather(*[c.fetchval('SELECT email FROM users WHERE id = $1', i) for i in range(1,4)])
    print(rows)
    await c.close()
asyncio.run(a())
with psycopg.connect("host=127.0.0.1 port=56600 user=postgres dbname=postgres", autocommit=True) as c:
    print('psycopg', c.execute("SELECT * FROM users WHERE id = %s", (42,)).fetchone())
    print('psycopg prepared binary', c.execute("SELECT id, meta FROM users WHERE id = %s", (43,), prepare=True, binary=True).fetchone())
    with c.transaction():
        c.execute("UPDATE users SET name='qq' WHERE id = 9")
        print('txn', c.execute("SELECT name FROM users WHERE id = %s", (9,)).fetchone())
        raise psycopg.Rollback()
    print('after', c.execute("SELECT name FROM users WHERE id = %s", (9,)).fetchone())
