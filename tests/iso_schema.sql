DROP SCHEMA IF EXISTS s1, s2, iso, pgcoalesce CASCADE;
CREATE SCHEMA s1; CREATE SCHEMA s2; CREATE SCHEMA iso; CREATE SCHEMA pgcoalesce;
-- same-named tables in two schemas (search_path test)
CREATE TABLE s1.items(id int PRIMARY KEY, v text NOT NULL);
CREATE TABLE s2.items(id int PRIMARY KEY, v text NOT NULL);
INSERT INTO s1.items SELECT g, 's1-' || g FROM generate_series(1,500) g;
INSERT INTO s2.items SELECT g, 's2-' || g FROM generate_series(1,500) g;
-- RLS multi-tenant table
CREATE TABLE iso.docs(id int PRIMARY KEY, tenant int NOT NULL, body text NOT NULL);
INSERT INTO iso.docs SELECT g, g % 3, 'doc-' || g || '-t' || (g % 3) FROM generate_series(1,500) g;
ALTER TABLE iso.docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_p ON iso.docs USING (tenant = nullif(current_setting('app.tenant_id', true), '')::int);
-- privileges differ per role
CREATE TABLE iso.secret(id int PRIMARY KEY, s text NOT NULL);
INSERT INTO iso.secret SELECT g, 'secret-' || g FROM generate_series(1,500) g;
-- GUC-sensitive output formats
CREATE TABLE iso.pub(id int PRIMARY KEY, ts timestamptz, d date, iv interval, f float8, b bytea, m numeric);
INSERT INTO iso.pub SELECT g, timestamptz '2026-01-01 12:00:00+00' + g * interval '7 hours 13 min', date '2026-03-01' + g,
  g * interval '1 day 2 hours 3 min', g / 7.0, decode(md5(g::text), 'hex'), g * 1.5 FROM generate_series(1,500) g;
-- security-barrier view and a volatile attribute-notation function
CREATE VIEW iso.v_sb WITH (security_barrier) AS SELECT id, f FROM iso.pub WHERE id % 2 = 0;
CREATE SEQUENCE iso.bump_seq;
CREATE FUNCTION iso.bump(iso.pub) RETURNS bigint LANGUAGE sql VOLATILE AS $$ SELECT nextval('iso.bump_seq') $$;
-- a function that changes search_path invisibly to text rules
CREATE FUNCTION iso.sneaky_path() RETURNS int LANGUAGE sql AS $$ SELECT 1 FROM (SELECT set_config('search_path', 's2, iso', false)) x $$;
CREATE FUNCTION iso.sneaky_tz() RETURNS int LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('timezone', 'Asia/Tokyo', false); RETURN 1; END $$;

GRANT USAGE ON SCHEMA s1, s2, iso TO app_a, app_b, app_md5;
GRANT SELECT ON s1.items, s2.items, iso.docs, iso.pub, iso.v_sb TO app_a, app_b, app_md5;
GRANT SELECT ON iso.secret TO app_a;          -- app_b may NOT read secret
GRANT SELECT, UPDATE, USAGE ON SEQUENCE iso.bump_seq TO app_a, app_b;
GRANT app_b TO app_a;                         -- app_a may SET ROLE app_b
ALTER ROLE app_a SET search_path = s1, iso;
ALTER ROLE app_b SET search_path = s2, iso;
ALTER ROLE app_md5 SET search_path = s1, iso;

-- auth_query helper (PgBouncer pattern): auth_reader can look up secrets without being superuser
CREATE FUNCTION pgcoalesce.get_auth(p_usename text, OUT usename name, OUT passwd text) RETURNS record
 LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS
 $$ SELECT usename, passwd FROM pg_catalog.pg_shadow WHERE usename = p_usename AND usename NOT IN ('postgres') $$;
REVOKE ALL ON FUNCTION pgcoalesce.get_auth(text) FROM PUBLIC;
GRANT USAGE ON SCHEMA pgcoalesce TO auth_reader;
GRANT EXECUTE ON FUNCTION pgcoalesce.get_auth(text) TO auth_reader;
ANALYZE;
