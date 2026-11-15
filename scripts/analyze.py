import json,statistics as st,collections,sys
def load(f): return [json.loads(l) for l in open(f)]
m=load('results_main.jsonl')
g=collections.defaultdict(list)
for d in m:
    k=d['label'].split(':')[1]; g[(d['dist'],k)].append(d)
print("MAIN 5000 req/1s, 500 conns (median of 3)")
print(f"{'dist':8}{'path':14}{'qps':>6}{'p50':>7}{'p99':>7}{'max':>7}{'cpu_ms':>8}{'us/req':>7}{'calls':>7}{'exec_ms':>8}{'err':>5}{'wrong':>6}  load")
base={}
for (dist,k),v in sorted(g.items()):
    med=lambda f: st.median(x[f] for x in v)
    row=dict(qps=med('achieved_qps'),p50=med('p50_ms'),p99=med('p99_ms'),mx=med('max_ms'),cpu=med('db_cpu_ms'),calls=med('pgss_calls'),ex=med('pgss_exec_ms'),err=sum(x['errors'] for x in v),wr=sum(x['wrong_rows'] for x in v))
    print(f"{dist:8}{k:14}{row['qps']:6.0f}{row['p50']:7.2f}{row['p99']:7.2f}{row['mx']:7.1f}{row['cpu']:8.0f}{row['cpu']/5:7.1f}{row['calls']:7.0f}{row['ex']:8.1f}{row['err']:5}{row['wr']:6}  {','.join(x['loadavg_before'] for x in v)}  cpu_all={[round(x['db_cpu_ms']) for x in v]}")
s=load('results_sweep.jsonl')
print("\nSWEEP 2s, 500 conns")
print(f"{'dist':8}{'rate':>6} {'path':14}{'qps':>7}{'p50':>8}{'p99':>8}{'cpu_ms':>8}{'us/req':>7}{'calls':>7}{'err':>6} load")
for d in s:
    print(f"{d['dist']:8}{d['rate']:6} {d['label'].split(':')[1]:14}{d['achieved_qps']:7.0f}{d['p50_ms']:8.2f}{d['p99_ms']:8.2f}{d['db_cpu_ms']:8.0f}{d['db_cpu_us_per_req']:7.1f}{d['pgss_calls']:7}{d['errors']:6} {d['loadavg_before']}")
