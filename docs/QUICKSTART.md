# pgcoalesce quickstart (DRAFT)

## 1. Build
```bash
git clone <repo-url> && cd pgcoalesce/src     # repo not published yet
CGO_ENABLED=0 go build -o pgcoalesce .
# or: docker build -t pgcoalesce -f Dockerfile .
```

## 2. Users
Same format as PgBouncer's `auth_file`; copy SCRAM verifiers from `pg_authid`:
```sql
SELECT format('"%s" "%s"', rolname, rolpassword) FROM pg_authid WHERE rolname = 'app';
```
Or skip the file and use `auth_query` with a SECURITY DEFINER lookup function (as in PgBouncer):
```sql
CREATE SCHEMA pgcoalesce;
CREATE FUNCTION pgcoalesce.get_auth(p_user text, OUT usename name, OUT passwd text)
  RETURNS record LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS
  $$ SELECT rolname, rolpassword FROM pg_authid WHERE rolname = p_user AND rolcanlogin $$;
REVOKE ALL ON FUNCTION pgcoalesce.get_auth(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcoalesce.get_auth(text) TO pgcoalesce_auth;
```

## 3. Config
Copy `pgcoalesce.example.ini` and set `server_addr` (coalesced batches) and `upstream_addr`
(everything else: Postgres or PgBouncer in **session** mode; see Safety).

## 4. Run and point your app at it
```bash
./pgcoalesce -config pgcoalesce.ini
# override any key: -set window=5ms -set min_expected=1
psql "postgres://app@localhost:6432/app?sslmode=require"
```

## 5. Watch it
```bash
curl -s localhost:9187/metrics | grep -E 'pgcoalesce_(coalesced|batch|tx_|verify)'
curl -s localhost:9187/stats        # JSON
kill -HUP <pid>                     # reload templates/engines (also POST /admin/reload)
kill -TERM <pid>                    # graceful: idle clients get 57P01, open transactions may finish
```
Healthy signs: `coalesced_reqs` grows with load, `verify_mismatch` stays near 0, `tx_bypass` is high for
per-user PostgREST traffic (expected — see Limits).
