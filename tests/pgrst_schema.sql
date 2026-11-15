DO $$BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticator') THEN CREATE ROLE authenticator LOGIN NOINHERIT; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated NOLOGIN; END IF;
END$$;
GRANT anon, authenticated TO authenticator;
DROP SCHEMA IF EXISTS api CASCADE;
CREATE SCHEMA api;
-- Supabase-style helper
CREATE FUNCTION api.uid() RETURNS int LANGUAGE sql STABLE AS $$ SELECT nullif(current_setting('request.jwt.claims', true)::json->>'sub', '')::int $$;
-- per-user rows behind RLS (the typical Supabase table)
CREATE TABLE api.profiles(id int PRIMARY KEY, owner int NOT NULL, display text NOT NULL, bio text, updated timestamptz NOT NULL DEFAULT now());
INSERT INTO api.profiles SELECT g, g % 2000, 'user-' || g, repeat('bio ', g % 7), timestamptz '2026-01-01' + g * interval '1 min' FROM generate_series(1, 100000) g;
ALTER TABLE api.profiles ENABLE ROW LEVEL SECURITY;
CREATE POLICY own ON api.profiles FOR SELECT TO authenticated USING (owner = api.uid());
-- public catalog, no RLS
CREATE TABLE api.products(id int PRIMARY KEY, name text NOT NULL, price numeric(10,2) NOT NULL);
INSERT INTO api.products SELECT g, 'product-' || g, (g % 1000) / 7.0 FROM generate_series(1, 100000) g;
GRANT USAGE ON SCHEMA api TO anon, authenticated;
GRANT SELECT ON api.products TO anon, authenticated;
GRANT SELECT ON api.profiles TO authenticated;
GRANT EXECUTE ON FUNCTION api.uid() TO anon, authenticated;
ANALYZE api.profiles; ANALYZE api.products;
