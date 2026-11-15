DROP SCHEMA IF EXISTS cq CASCADE; CREATE SCHEMA cq;
SET search_path = cq;
CREATE TABLE t_int (id int4 PRIMARY KEY, a text, b int, c jsonb, d timestamptz, e numeric(12,3), f bytea, g bool, h float8, i int[]);
INSERT INTO t_int SELECT g,
  CASE WHEN g%7=0 THEN NULL ELSE 'txt'||g||CASE WHEN g%11=0 THEN ' "q" \ ,{}' ELSE '' END END,
  CASE WHEN g%5=0 THEN NULL ELSE g*3 END,
  CASE WHEN g%9=0 THEN NULL ELSE jsonb_build_object('g',g,'arr',jsonb_build_array(g,'x',null)) END,
  '2020-01-01'::timestamptz + g*interval '37 min',
  g/7.0, decode(md5(g::text),'hex'), g%2=0, g/3.0, ARRAY[g,g+1]
FROM generate_series(-50, 5000) g;
CREATE TABLE t_big (id int8 PRIMARY KEY, a text, b int);
INSERT INTO t_big SELECT g*1000003::int8*1000, 'big'||g, g FROM generate_series(1,3000) g;
INSERT INTO t_big VALUES (9223372036854775807,'max',0),(-9223372036854775808,'min',0);
CREATE TABLE t_uuid (id uuid PRIMARY KEY, a text, b int);
INSERT INTO t_uuid SELECT md5('u'||g)::uuid, 'u'||g, g FROM generate_series(1,3000) g;
CREATE TABLE t_text (id serial PRIMARY KEY, k text UNIQUE NOT NULL, a text);
INSERT INTO t_text(k,a) SELECT 'key'||g, 'v'||g FROM generate_series(1,3000) g;
INSERT INTO t_text(k,a) VALUES ('', 'empty'), ('NULL','nullstr'), ('a"b','quote'), ('back\slash','bs'), ('c,o{m}m,a','punct'), ('  spaced  ','sp'), ('ünïcødé ✓','uni'), ('null','lower'), ('''single''','sq');
CREATE TABLE t_vc (k varchar(20) PRIMARY KEY, a text);
INSERT INTO t_vc SELECT 'vc'||g, 'a'||g FROM generate_series(1,500) g;
CREATE TABLE t_ch (k char(5) PRIMARY KEY, a text);
INSERT INTO t_ch SELECT 'c'||g, 'a'||g FROM generate_series(1,500) g;
CREATE TABLE t_nouniq (k int, a text); CREATE INDEX ON t_nouniq(k);
INSERT INTO t_nouniq SELECT g%100, 'n'||g FROM generate_series(1,1000) g;
CREATE VIEW v_int AS SELECT id, a, b FROM t_int;
CREATE TABLE t_rls (id int PRIMARY KEY, a text); ALTER TABLE t_rls ENABLE ROW LEVEL SECURITY;
INSERT INTO t_rls SELECT g, 'r'||g FROM generate_series(1,100) g;
CREATE TABLE "Mixed Case" ("Id" int PRIMARY KEY, "Val" text, v int, o int);
INSERT INTO "Mixed Case" SELECT g, 'm'||g, g, -g FROM generate_series(1,500) g;
CREATE TABLE t_rw (id int PRIMARY KEY, val int NOT NULL);
INSERT INTO t_rw SELECT g, 0 FROM generate_series(1,1000) g;
ANALYZE;
