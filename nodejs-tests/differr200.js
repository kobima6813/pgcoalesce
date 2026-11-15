// Differential test with untyped text params (node-pg) incl. invalid values → per-client error propagation inside batches
const { Client } = require('pg');
const SEED = +process.argv[2] || 1, DUR = (+process.argv[3] || 20) * 1000;
let s = SEED; const rnd = () => (s = (s * 1103515245 + 12345) % 2147483648) / 2147483648;
const pick = a => a[Math.floor(rnd() * a.length)];
const SH = [
  ['SELECT * FROM t_int WHERE id = $1', () => pick([String(Math.floor(rnd() * 5000)), 'abc', '9999999999', '1.5', '', ' 42 ', '-0', '007', null, '2147483647', String(Math.floor(rnd()*20))])],
  ['SELECT a, b FROM t_int WHERE id = $1', () => pick([String(Math.floor(rnd() * 5000)), 'x1', null, '12'])],
  ['SELECT * FROM t_uuid WHERE id = $1', () => pick(['zzz', '00000000-0000-0000-0000-000000000000', 'not-a-uuid', 'C4CA4238-A0B9-2382-0DCC-509A6F75849B', '{c4ca4238-a0b9-2382-0dcc-509a6f75849b}'])],
  ['SELECT * FROM t_text WHERE k = $1', () => pick(['key1', 'key' + Math.floor(rnd() * 3000), 'a"b', 'back\\slash', 'c,o{m}m,a', '', 'NULL', null, '\u0000'.slice(1)])],
  ['SELECT * FROM t_big WHERE id = $1', () => pick(['1000003000', '99999999999999999999', 'abc', '-9223372036854775808'])],
  ['SELECT * FROM t_ch WHERE k = $1', () => pick(['c1', 'c1   ', 'c12', 'toolongvalue', 'c3 '])],
];
async function one(c, sql, v) {
  try { const r = await c.query({ text: sql, values: [v], rowMode: 'array' }); return JSON.stringify(['OK', r.fields.map(f => f.name + ':' + f.dataTypeID), r.rows]); }
  catch (e) { return JSON.stringify(['ERR', e.code, e.message]); }
}
(async () => {
  const N = 200; let ops = 0, mism = 0, errs = 0; const ex = [];
  const end = Date.now() + DUR;
  await Promise.all([...Array(N).keys()].map(async i => {
    const p = new Client({ host: '127.0.0.1', port: 56600, user: 'postgres', database: 'postgres' });
    const d = new Client({ host: '127.0.0.1', port: 55433, user: 'postgres', database: 'postgres' });
    await p.connect(); await d.connect();
    while (Date.now() < end) {
      const [sql, g] = pick(SH); const v = g();
      const [a, b] = [await one(p, sql, v), await one(d, sql, v)];
      ops++; if (a.startsWith('["ERR')) errs++;
      if (a !== b) { mism++; if (ex.length < 10) ex.push({ sql, v, a: a.slice(0, 200), b: b.slice(0, 200) }); }
    }
    await p.end(); await d.end();
  }));
  console.log(JSON.stringify({ seed: SEED, ops, errors_compared: errs, mismatches: mism, examples: ex }, null, 1));
})();
