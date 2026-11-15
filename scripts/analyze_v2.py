import json, sys, statistics as st, collections
f = sys.argv[1] if len(sys.argv) > 1 else 'results_v2.jsonl'
rows = [json.loads(l) for l in open(f)]
g = collections.defaultdict(list)
for d in rows:
    g[(d['label'].split(':')[0], d['mode'], d['dist'], d['rate'], d['label'].split(':')[1])].append(d)
print(f"{'phase':14}{'mode':9}{'dist':8}{'rate':>6} {'path':10}{'n':>2}{'qps':>7}{'p50':>7}{'p99':>8}{'dbCPUus/req':>12}{'proxyCPUus/req':>15}{'calls':>7}{'err':>4}{'wrong':>6}")
for k in sorted(g, key=lambda k: (k[0], k[1], k[2], k[3], k[4])):
    v = g[k]; m = lambda x: st.median(d[x] for d in v)
    n = v[0]['n']
    print(f"{k[0]:14}{k[1]:9}{k[2]:8}{k[3]:6} {k[4]:10}{len(v):2}{m('achieved_qps'):7.0f}{m('p50_ms'):7.2f}{m('p99_ms'):8.2f}{m('db_cpu_ms')*1000/n:12.1f}{m('proxy_cpu_ms')*1000/n:15.1f}{m('pgss_calls'):7.0f}{sum(d['errors'] for d in v):4}{sum(d['wrong_rows'] for d in v):6}")
