"""Operational checks: cancel forwarding, ALTER ROLE semantics, graceful shutdown, TLS-required mode, metrics."""
import psycopg, threading, time, subprocess, signal, json, urllib.request, os
PW = {u: open('/workspace/coalesce/secrets/%s.pw' % u).read().strip() for u in ('app_a',)}
SEC = dict(host='127.0.0.1', port=56610, user='app_a', password=PW['app_a'], dbname='postgres', sslmode='require', autocommit=True)
SU = dict(host='127.0.0.1', port=55433, user='postgres', dbname='postgres', autocommit=True)
res = {}

# 1. cancel of a forwarded query reaches the right backend
c = psycopg.connect(**SEC)
t0 = time.time()
threading.Timer(0.5, c.cancel).start()
try:
    c.execute('SELECT pg_sleep(10)'); res['cancel'] = 'FAIL: not canceled'
except psycopg.errors.QueryCanceled:
    res['cancel'] = 'ok (%.2fs)' % (time.time() - t0)
c.execute('SELECT 1')  # connection still usable
# a cancel that arrives while idle is ignored, as in Postgres
c.cancel(); time.sleep(0.2)
res['cancel_idle_ignored'] = 'ok' if c.execute('SELECT v FROM items WHERE id = 3').fetchone()[0] == 's1-3' else 'FAIL'
c.close()

# 2. ALTER ROLE ... SET: new sessions see it, existing sessions keep theirs (Postgres semantics)
a1 = psycopg.connect(**SEC)
q = 'SELECT v FROM items WHERE id = %s'
before = a1.execute(q, (7,)).fetchone()[0]
su = psycopg.connect(**SU)
su.execute('ALTER ROLE app_a SET search_path = s2, iso')
a2 = psycopg.connect(**SEC)
new = a2.execute(q, (7,)).fetchone()[0]
old = a1.execute(q, (7,)).fetchone()[0]
su.execute('ALTER ROLE app_a SET search_path = s1, iso')
a3 = psycopg.connect(**SEC)
back = a3.execute(q, (7,)).fetchone()[0]
res['alter_role'] = 'ok' if (before, new, old, back) == ('s1-7', 's2-7', 's1-7', 's1-7') else 'FAIL %r' % ((before, new, old, back),)
for x in (a1, a2, a3, su): x.close()

# 3. graceful shutdown: idle clients get 57P01, a client inside a transaction may finish it
p = subprocess.Popen(['/workspace/coalesce/proxy3/pgcoalesce', '-config', '/workspace/coalesce/conf3/bench2ms.ini',
                      '-set', 'listen_addr=127.0.0.1:56630', '-set', 'metrics_addr=127.0.0.1:56730', '-set', 'shutdown_timeout=10'],
                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
time.sleep(1)
D = dict(host='127.0.0.1', port=56630, user='postgres', dbname='postgres', autocommit=True)
idle = psycopg.connect(**D); idle.execute('SELECT 1')
intx = psycopg.connect(**D); intx.execute('BEGIN'); intx.execute('SELECT 1')
t0 = time.time(); p.send_signal(signal.SIGTERM); time.sleep(0.5)
try:
    idle.execute('SELECT 1'); r_idle = 'FAIL: still open'
except psycopg.OperationalError as e:
    r_idle = 'ok (closed: %s)' % (str(e).strip().splitlines()[0][:70])
try:
    intx.execute('SELECT 2'); intx.execute('COMMIT'); r_tx = 'ok (finished its transaction)'
except Exception as e:
    r_tx = 'FAIL %s' % e
try:
    psycopg.connect(**D, connect_timeout=2); r_new = 'FAIL: accepted'
except psycopg.OperationalError:
    r_new = 'ok (refused)'
p.wait(timeout=15)
res['shutdown'] = {'idle_client': r_idle, 'client_in_tx': r_tx, 'new_connections': r_new, 'exit_code': p.returncode, 'secs': round(time.time() - t0, 2)}

# 4. client_tls_sslmode=require
p = subprocess.Popen(['/workspace/coalesce/proxy3/pgcoalesce', '-config', '/workspace/coalesce/conf3/secure.ini',
                      '-set', 'listen_addr=127.0.0.1:56631', '-set', 'metrics_addr=127.0.0.1:56731', '-set', 'client_tls_sslmode=require'],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1)
try:
    psycopg.connect(**dict(SEC, port=56631, sslmode='disable')); r1 = 'FAIL: plaintext accepted'
except psycopg.OperationalError:
    r1 = 'ok (plaintext rejected)'
x = psycopg.connect(**dict(SEC, port=56631)); r2 = 'ok (TLS accepted)' if x.info.pgconn.ssl_in_use else 'FAIL'; x.close()
p.terminate(); p.wait(timeout=15)
res['client_tls_require'] = [r1, r2]

# 5. metrics
m = urllib.request.urlopen('http://127.0.0.1:56710/metrics').read().decode()
res['metrics'] = [l for l in m.splitlines() if l.startswith(('pgcoalesce_coalesce_ratio', 'pgcoalesce_batch_size_count', 'pgcoalesce_request_seconds_count', 'pgcoalesce_window_seconds', 'pgcoalesce_cancel_requests_total'))]
print(json.dumps(res, indent=1))
