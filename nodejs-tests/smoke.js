const { Client } = require('pg');
(async () => {
  const c = new Client({ host: '127.0.0.1', port: 56600, user: 'postgres', database: 'postgres' });
  await c.connect();
  let r = await c.query('SELECT * FROM users WHERE id = $1', [42]); console.log('pg param', r.rows[0].email, r.rows.length);
  r = await c.query({ name: 'byid', text: 'SELECT name, id FROM users WHERE id = $1', values: [7] }); console.log('pg named', r.rows[0]);
  r = await c.query({ name: 'byid', text: 'SELECT name, id FROM users WHERE id = $1', values: [8] }); console.log('pg named2', r.rows[0]);
  r = await c.query('SELECT * FROM users WHERE id = $1', [null]); console.log('null', r.rows.length);
  try { await c.query('SELECT * FROM users WHERE id = $1', ['abc']); } catch (e) { console.log('err', e.code, e.message); }
  await c.query('BEGIN'); await c.query("UPDATE users SET name='zz' WHERE id = 3");
  r = await c.query('SELECT name FROM users WHERE id = $1', [3]); console.log('in txn', r.rows[0]);
  await c.query('ROLLBACK');
  r = await c.query('SELECT name FROM users WHERE id = $1', [3]); console.log('after rb', r.rows[0]);
  await c.end();
  process.env.DATABASE_URL = 'postgresql://postgres@127.0.0.1:56600/postgres';
  const { PrismaClient } = require('@prisma/client');
  const p = new PrismaClient();
  const u = await p.users.findUnique({ where: { id: 5 } }); console.log('prisma', u.email, u.meta);
  const many = await Promise.all([1,2,3,4,5,6,7,8].map(i => p.users.findUnique({ where: { id: i } }))); console.log('prisma many', many.map(x => x.id).join(','));
  await p.$disconnect();
})().catch(e => { console.error('FAIL', e); process.exit(1); });
