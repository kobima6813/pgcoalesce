// Open-loop load with node-postgres (real driver: unnamed extended-protocol statements, text formats)
const { Client } = require('pg'); const fs = require('fs');
const [port, rate, durS, label, pids] = [+process.argv[2], +process.argv[3], +process.argv[4], process.argv[5], (process.argv[6] || '').split(',').filter(Boolean)];
const cpu = () => +fs.readFileSync('/sys/fs/cgroup/coalesce_pg/cpu.stat', 'utf8').match(/usage_usec (\d+)/)[1];
const pcpu = () => pids.reduce((a, p) => { try { const f = fs.readFileSync(`/proc/${p}/stat`, 'utf8').split(') ')[1].split(' '); return a + (+f[11] + +f[12]) * 10; } catch { return a; } }, 0);
(async () => {
  const N = 500, conns = [];
  for (let i = 0; i < N; i += 50) await Promise.all([...Array(50)].map(async () => { const c = new Client({ host: '127.0.0.1', port, user: 'postgres', database: 'postgres' }); await c.connect(); conns.push(c); }));
  for (const c of conns) await c.query('SELECT * FROM users WHERE id = $1', [1]);
  const idle = [...conns], queue = []; const lat = []; let errs = 0, wrong = 0;
  const total = rate * durS; let seeded = 1;
  const rnd = () => (seeded = (seeded * 1103515245 + 12345) % 2147483648) / 2147483648;
  const issue = (c, job) => c.query('SELECT * FROM users WHERE id = $1', [job.id]).then(r => { if (!r.rows[0] || r.rows[0].email !== `user${job.id}@ex.com`) wrong++; }, () => errs++).finally(() => { lat.push(Number(process.hrtime.bigint() - job.t) / 1e6); const n = queue.shift(); if (n) issue(c, n); else idle.push(c); });
  await new Promise(r => setTimeout(r, 300));
  const c0 = cpu(), p0 = pcpu(); const start = process.hrtime.bigint(); let sent = 0;
  await new Promise(done => {
    const tick = () => {
      const due = Math.min(total, Math.floor(Number(process.hrtime.bigint() - start) / 1e9 * rate));
      while (sent < due) { const job = { id: Math.floor(rnd() * 1000000) + 1, t: start + BigInt(Math.round(sent * 1e9 / rate)) }; sent++; const c = idle.pop(); if (c) issue(c, job); else queue.push(job); }
      if (sent < total) setImmediate(tick); else { const w = () => lat.length >= total ? done() : setTimeout(w, 5); w(); }
    };
    tick();
  });
  const el = Number(process.hrtime.bigint() - start) / 1e9;
  await new Promise(r => setTimeout(r, 300));
  const db = (cpu() - c0) / 1000, px = pcpu() - p0;
  lat.sort((a, b) => a - b); const q = p => lat[Math.floor(p * (lat.length - 1))];
  console.log(JSON.stringify({ label, driver: 'node-pg', rate, n: total, achieved_qps: total / el, p50_ms: q(.5), p99_ms: q(.99), max_ms: q(1), db_cpu_ms: db, db_cpu_us_per_req: db * 1000 / total, proxy_cpu_ms: px, errors: errs, wrong_rows: wrong }));
  await Promise.all(conns.map(c => c.end()));
})();
