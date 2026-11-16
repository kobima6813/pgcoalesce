# pgcoalesce

**A Postgres wire-protocol proxy that merges concurrent primary-key reads into one query, and fails closed when it can't prove that is safe.**

> Status: 0.3.0, experimental. Benchmarked on one machine only, with no production users yet. Apache-2.0.

Serverless, edge and API backends send many tiny `SELECT ... WHERE id = $1` reads at the same moment. A pooler such as PgBouncer still runs each one as its own statement. pgcoalesce holds these reads for up to a couple of milliseconds, then runs **one** statement for all the keys:

```sql
SELECT k.ord, x.* FROM unnest($1::int8[]) WITH ORDINALITY k(v, ord)
CROSS JOIN LATERAL (<your exact query with $1 -> k.v>) x
```

Each client then receives exactly its own rows, in its own requested format.

- **No cache.** Every answer comes from Postgres at that moment, with no TTLs and no staleness.
- **No code changes.** Only the connection string changes.

## Who it is for (and who it is not for)
- **Gains start around 1,000 point reads per second** for a single query shape. Below that, the adaptive window rarely finds a partner, so requests go straight through with no added wait.
- **Uniform keys save less than skewed ones.** In our benchmark (a 172 MB table, larger than the 64 MB `shared_buffers`, inside a 256 MB limit) uniform keys saved 70–84% DB CPU and skewed (Zipf) keys 79–91%. Page access costs the same whether a key is fetched alone or in a batch, so expect lower savings when the working set does not fit in memory.
- **You pay latency:** p50 rises by about the window, +1.8 ms with a 2 ms window and about +3.4 ms with 5 ms. Do not use it for latency-critical single reads.
- **The proxy itself costs CPU:** about 41–49 µs per request at 5k/s, versus about 27–30 µs for PgBouncer. The two reach parity around 20k/s. You are moving work off the database, which scales vertically, onto a stateless tier.

## Benchmark (one box, 15 Nov 2026)
**Setup:**
- Postgres 17.12 in a cgroup limited to 1 CPU and 256 MB (`shared_buffers=64MB`), with a 1M-row `users` table (172 MB).
- 500 connections, open-loop load, pgx v5. Baseline: PgBouncer 1.24.1 in transaction mode with pool=4 (pool=20 was worse).
- DB CPU is measured from the cgroup. Values are medians of 3 runs. Each run is short: 10,000 requests, about 2 seconds, and single runs differ by roughly ±20%.
- The 5k/s table was measured again in two later sessions, one of them on the current code. Named prepared, uniform keys, 2 ms window: 18.4 and 18.3 µs against 60.2 and 59.2 µs for PgBouncer. Raw data is in `results/`.

**DB CPU per request at 5,000 reads/s:**

| driver mode | keys | PgBouncer | pgcoalesce 2 ms | pgcoalesce 5 ms | p50 / p99 (ms): PgB → 2 ms → 5 ms |
|---|---|---|---|---|---|
| named prepared, binary | uniform | 57.8 µs | 17.5 µs (−70%) | 12.3 µs (−79%) | 0.59/1.7 → 2.41/4.2 → 3.98/7.3 |
| named prepared, binary | Zipf 1.1 | 54.8 µs | 11.7 µs (−79%) | 6.9 µs (−87%) | 0.58/1.6 → 2.33/4.0 → 3.96/7.3 |
| unnamed (exec) | uniform | 88.3 µs | 17.1 µs (−81%) | 14.0 µs (−84%) | 0.59/1.6 → 2.46/4.1 → 4.06/8.4 |
| unnamed (exec) | Zipf 1.1 | 82.9 µs | 12.3 µs (−85%) | 7.3 µs (−91%) | 0.60/1.5 → 2.33/4.0 → 3.94/7.3 |

**Uniform reads at lower rates (2 ms window):**
- **1k/s:** DB CPU 92.9 → 39.4 µs (−58%); p50 0.79 → 2.32 ms.
- **250/s:** 169.5 → 95.8 µs, with p50 1.46 → 1.21 ms. This gain comes from pgcoalesce's long-lived prepared templates, **not** from batching.

Larger windows save more and cost more latency. One 20k/s run with the 5 ms window showed a 1.5 s p99 tail under host contention. The 2 ms window is the default.

### PostgREST / Supabase (transaction-template coalescing)
PostgREST wraps every request in `BEGIN; set_config(role, claims, headers…, true); <one SELECT>; COMMIT`. pgcoalesce recognises that exact shape. Transactions with an **identical** fingerprint share one real transaction:
- The fingerprint covers role, `search_path`, JWT claims, headers, method, path and every watched GUC.
- Every client gets protocol-exact answers, including `set_config` return values and command tags.

The test used real PostgREST 16.5, an RLS-protected `profiles` table, `GET /x?id=eq.N`, and DB CPU per request (proxied vs direct):

| scenario | 1k req/s | 3k req/s |
|---|---|---|
| S1: anonymous catalog reads (identical claims) | 149 vs 199 µs (−25%) | 70 vs 165 µs (−58%) |
| S2: 2,000 logged-in users (Zipf), stable token per user | 228 vs 216 µs (parity) | 186 vs 176 µs (7% coalesced, parity) |
| S3: unique claims per request (`iat`/`jti`) | 198 vs 224 µs (0% coalesced) | 181 vs 175 µs (0% coalesced) |

**The honest takeaway:**
- It helps anonymous and public reads.
- It does almost nothing for per-user traffic, because every user's claims and headers differ.
- Cold fingerprints are detected and passed straight through, so you don't pay for a shared transaction of one.
- We do **not** normalise claims such as `iat` or `exp`. Policies and functions can read any claim, so merging them is not provably safe.

## Bounding the cost of batching
Two settings limit what a client can lose by sharing a batch with others.

**Adaptive window (`min_expected`, default 1).** For each query shape the proxy estimates the arrival rate from the last 32 lookups. A lookup is held only if at least `min_expected` more lookups of that shape are expected within `window`. Otherwise it is sent at once, so quiet periods pay no added latency. `min_expected = 0` always holds for the full window. Immediate dispatches are counted in `pgcoalesce_adaptive_immediate_total`.

Measured on the benchmark box on 16 Nov 2026 (100 uniform reads/s, named prepared, 2 ms window, 3 runs of 6 s, medians):

| | p50 | p99 |
|---|---|---|
| PgBouncer (pool 4) | 1.50 ms | 2.82 ms |
| pgcoalesce, fixed window (`min_expected=0`) | 3.55 ms | 5.61 ms |
| pgcoalesce, adaptive window (default) | 1.22 ms | 2.24 ms |

**Per-batch timeout (`batch_timeout`, default 500 ms, 0 = off).** Every client in a batch shares its fate: one slow plan, lock wait or `statement_timeout` would hit all of them. When a batch of two or more keys runs longer than `batch_timeout`, the proxy sends a CancelRequest for it, retires that server connection (so a late cancel can never reach another statement), and re-runs each request on its own client session, exactly as it would run without pgcoalesce. A slow or blocked key then delays only its own client. Batches that fail with `statement_timeout`, `lock_timeout`, a deadlock or `transaction_timeout` are re-run the same way instead of key by key on the shared connection. Other batch errors are still isolated by re-running each key on the coalescer connection, so an error reaches only the client whose key caused it. Shared transactions (`tx_coalesce`) are bounded by the same timeout and replay on each client's own session. Watch `pgcoalesce_batch_timeouts_total` and `pgcoalesce_batch_retried_requests_total`.

At 5,000 reads/s neither change cost DB CPU savings. Same box, same session, medians of 3 runs, DB CPU per request against PgBouncer (pool 4): named prepared, uniform keys 19.1 vs 68.5 µs (−72%), Zipf 11.9 vs 54.4 µs (−78%); unnamed (`exec`) uniform 17.2 vs 91.1 µs (−81%), Zipf 12.1 vs 84.2 µs (−86%). The previous release measured 18.7 / 11.9 / 17.4 / 12.3 µs in the same runs. 0 wrong rows, 0 errors. No batch hit the timeout in these runs.

## Safety model: fail closed
A read is coalesced only when every check passes. Anything unrecognised is relayed unchanged.

1. **Exact shape.** `SELECT <plain columns|*> FROM <table> WHERE <unique col> = $1`. No functions, casts, ORDER BY or LIMIT, no extra predicates, no `FOR UPDATE/SHARE`.
2. **Catalog-verified.** Ordinary or partitioned, non-temporary table; single-column valid unique index; **RLS off** (for plain coalescing); no views (so no security-barrier views). Every output column is a real column of that table. Key and output types are built-in. `=` must resolve to a `pg_catalog` operator backed by an immutable function.
3. **Identical result-affecting context.** Clients share a batch only when the following match:
   - database;
   - `current_user` and `session_user`;
   - every session-level setting listed in `pg_settings`: `search_path`, TimeZone, DateStyle, IntervalStyle, `extra_float_digits`, `bytea_output`, `client_encoding`, `standard_conforming_strings`, role and the rest;
   - temp-schema state.

   Custom settings such as `app.tenant_id` are **not** part of this fingerprint, because `pg_settings` does not list them (checked on Postgres 16 and 17.12). Two sessions that differ only in a custom setting can share a plain batch. That cannot change the answer: only RLS policies, views and functions can read a custom setting, and all three are excluded from plain coalescing. In the transaction path, the custom settings that policies and functions read are compared explicitly.

   After any non-neutral passthrough statement, the client's own session is re-fingerprinted. A changed client moves to a separate engine whose connections replay its settings, and the fingerprint must match exactly or the client is passed through. `ALTER ROLE/DATABASE ... SET` is detected on every connect.
4. **Transactions.** Plain coalescing happens only when the client is idle, never inside a transaction block. Transaction-template coalescing accepts only the exact PostgREST shape. Any deviation replays the client's transaction on its own session:
   - writes, savepoints, a second statement or errors;
   - user functions that write GUCs, or read them dynamically;
   - RLS policies that call **volatile** functions.
5. **Errors stay per-client.** If a batch fails (for example, one client sent `'abc'` for an int key), it is re-run key by key, so each client gets the exact SQLSTATE and message it would have seen directly.

**Adversarial isolation suites** compare every response, byte for byte, with direct Postgres while clients hammer concurrently. Setup:
- Two tenants with RLS and different GRANTs.
- `SET ROLE` and `set_config('app.tenant_id')`.
- Same-named tables in different schemas behind different `search_path` values.
- TimeZone/DateStyle differences, volatile functions, a security-barrier view.
- PostgREST personas with differing claims and headers, savepoints, writes, and bad keys.

Numbers below are from the current code, with the race detector on. Every suite ran twice: once with default settings and once with batching forced (`min_expected=0`, `window=15ms`), because with default settings the test clients rarely overlap. "Merged batches" counts executed batches that held two or more distinct keys or transactions, which is the only place a mix-up between clients could happen.

| suite | operations | merged batches | cross-client leaks / mismatches |
|---|---|---|---|
| isolation (roles, RLS, search_path, GUCs) | 621,794 | 27,276 | **0** |
| differential (asyncpg, psycopg text/binary/pipeline/simple, transactions) | 177,181 | 9,816 | **0** |
| PostgREST transaction suite | 153,314 transactions | 10,489 | **0** |
| node-postgres with invalid values mixed into batches | 648,233 (167,171 error comparisons) | 83,664 | **0** |
| volatile-function RLS policy | 39 transactions | 0 (correctly refused) | **0** |

The suite caught one real bug before release. A PostgREST connection that prepared a statement inside a coalescible transaction and then deviated got `42P05 prepared statement already exists`. It is fixed, and that exact sequence is now a regression test.

A pre-launch code review then found four more, all fixed and covered by `tests/review_regress.py`:
- Text keys sent in a non-UTF-8 `client_encoding` (for example LATIN1) were corrupted when they shared a batch, so existing rows came back as "not found". Batches are now built byte for byte; client-only encodings (SJIS, BIG5, GBK, ...) run key by key.
- A `Close` followed by a reference to the closed statement or portal in the same message group crashed the whole proxy. It now returns the Postgres error, and an internal error on one connection can no longer take the others down.
- Every distinct session configuration kept its server connections forever. Engines are now limited (`max_engines`) and closed when idle (`engine_idle_timeout`).
- A simple-protocol string literal containing a backslash was read as if `standard_conforming_strings` were on. Such literals are now passed through.

**Batch timeout (16 Nov 2026).** Go unit tests (`go test -race ./...`) cover the cancel path against a fake server (cancel delivered, lost cancel, fast batches untouched, server-side `statement_timeout`, per-key error isolation, shared transactions). `tests/batch_timeout_test.py` checks it live: with an ACCESS EXCLUSIVE lock held on the table for 1.2 s, 60 concurrent clients (extended and simple protocol) all got exactly their own row, 2 batches were canceled and 22 requests re-run individually; 2,000 lookups afterwards ran with no timeouts and 0 mismatches. The suites above were re-run on this code with the race detector (40 s each, default and forced batching): 0 mismatches in all of them.

### Residual risks (read before deploying)
- **C-language functions that read custom GUCs.** These are invisible to the GUC watch-list scan, which only reads SQL/PL source and policies. If an RLS policy or view on a transaction-coalesced table relies on one, disable `tx_coalesce`.
- **STABLE functions that call volatile ones.** Postgres allows this; it is not checked recursively.
- **Transaction-mode pooler as `upstream_addr`.** Passthrough statements may land on different server sessions, so per-client session-state verification is meaningless. Use Postgres directly or a session-mode pooler if your clients use SET, temp tables or `set_config`.
- **`ALTER ROLE ... SET` window.** A change takes effect for coalescing at the client's next connect or engine refresh. Postgres itself applies it only to new sessions.
- **Ignored session features.** Advisory locks and `LISTEN` don't change results but are not coalesced.
- **Protocol coverage.** Transaction-template coalescing supports the extended protocol only (as PostgREST uses). Simple-protocol transactions pass through.
- **Server connections per session configuration.** Each distinct combination of user, database, startup options and session-level settings (for example one `search_path` per tenant) gets its own engine with `lanes` server connections. `max_engines` bounds this (default 32) and idle engines are closed after `engine_idle_timeout`; variants beyond the limit are served by passthrough. With many tenants active at once, most of them will not be coalesced.
- **Metrics endpoint is unauthenticated.** `/metrics`, `/stats` and `POST /admin/reload` have no authentication. Bind `metrics_addr` to loopback or a private interface.
- **Server-side TLS default.** `server_tls_sslmode` defaults to `prefer`, which does not verify the server certificate. Use `verify-full` for anything that is not on localhost.
- **Schema changes.** Query templates are cached for `template_ttl` (5 minutes by default). Behaviour right after a schema change on a coalesced table (for example `ADD COLUMN` under `SELECT *`) has not been tested.
- **Batch timeouts under overload.** When a batch times out, its requests run individually, which costs the database more than the batch would have. If batches routinely exceed `batch_timeout` because the database is saturated, the retries add load. Size `batch_timeout` well above your normal batch round trip (`pgcoalesce_batch_exec_seconds`).
- **Limited testing.** One machine, synthetic schemas, and no third-party security review. Treat it as beta.

## Production features
- **Client side:** SCRAM-SHA-256 and md5 auth, using a PgBouncer-format `auth_file` or `auth_query` through a `SECURITY DEFINER` function. TLS (`allow`/`require`).
- **Server side:** TLS `disable` through `verify-full`, plus SCRAM and md5.
- **CancelRequest:** forwarded with per-client keys; a pending coalesced request is answered with 57014. A batch running past `batch_timeout` is canceled on the server and its requests re-run individually.
- **Shutdown:** graceful on SIGTERM. Idle clients get 57P01, open transactions finish, and a hard timeout applies. SIGHUP or `POST /admin/reload` reloads.
- **Observability:** Prometheus `/metrics` with coalesce ratio, batch-size and window histograms, latency, `tx_*` counters and verification counters. `/stats` gives JSON; pprof is optional.
- **Config:** one INI file, with `-set key=value` overrides. See [docs/QUICKSTART.md](docs/QUICKSTART.md) and `pgcoalesce.example.ini`.

## How it compares
- **PgBouncer, Supavisor, PgCat, RDS Proxy:** connection pooling, which pgcoalesce does not replace. Put pgcoalesce in front, or use one of them as `upstream_addr` (session mode).
- **Application-level DataLoader:** the same idea, but per request and inside your code. pgcoalesce batches across all clients and needs no code changes.
- **Read caches** (Redis, ReadySet, PolyScale): faster for hot keys, but they serve stale or derived data. pgcoalesce never caches.

## License
Apache-2.0.
