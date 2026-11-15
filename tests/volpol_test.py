import psycopg, json, urllib.request, sys
pw=open('/workspace/coalesce/secrets/authenticator.pw').read().strip()
st=lambda: json.loads(urllib.request.urlopen('http://127.0.0.1:56713/stats').read())
c=psycopg.connect(host='127.0.0.1',port=56613,user='authenticator',password=pw,dbname='rest',sslmode='require',autocommit=True)
c.execute("SET client_encoding = 'UTF8'"); 
def tx(table, k):
    c.execute("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY")
    c.execute("select set_config('search_path', %s, true), set_config('role', %s, true), set_config('request.jwt.claims', %s, true), set_config('request.method', %s, true), set_config('request.path', %s, true), set_config('request.headers', %s, true), set_config('request.cookies', %s, true)", ['"api", "public"','anon','{"role":"anon"}','GET','/'+table,'{}','{}'])
    cols = '"api"."%s"."id", "api"."%s"."%s"' % (table, table, 'name' if table=='products' else 'v')
    r=c.execute('WITH pgrst_source AS ( SELECT %s FROM "api"."%s" WHERE  "api"."%s"."id" = %%s  ) SELECT null::bigint AS total_result_set, pg_catalog.count(_postgrest_t) AS page_total, coalesce(json_agg(_postgrest_t), \'[]\') AS body, nullif(current_setting(\'response.headers\', true), \'\') AS response_headers, nullif(current_setting(\'response.status\', true), \'\') AS response_status, \'\' AS response_inserted FROM ( SELECT * FROM pgrst_source ) _postgrest_t' % (cols, table, table), [k]).fetchall()
    c.execute("COMMIT"); return r
for t in ['products','audited']:
    a=st(); res=[tx(t,k) for k in range(1,40)]; b=st()
    print(t, 'rows_ok', all(r[0][1]==1 for r in res), 'tx_coalesced+', b['tx_coalesced']-a['tx_coalesced'], 'begins+', b['tx_begins']-a['tx_begins'], 'fallbacks+', b['tx_fallbacks']-a['tx_fallbacks'])
