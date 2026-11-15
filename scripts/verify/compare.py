import json, sys, statistics as st, collections
files = sys.argv[1:]
data = {}
for f in files:
    g = collections.defaultdict(list)
    for l in open(f):
        l = l.strip()
        if not l: continue
        r = json.loads(l)
        g[(r['label'], r['dist'], r['rate'])].append(r)
    data[f] = g
keys = sorted(set().union(*[set(g) for g in data.values()]), key=lambda k: (k[0].split(':')[0], k[2], k[1], k[0]))
hdr = ['label', 'dist', 'rate'] + [f'{n}:{m}' for n in range(len(files)) for m in ('dbus', 'p50', 'p99', 'err')]
print('| ' + ' | '.join(hdr) + ' |'); print('|' + '---|' * len(hdr))
for k in keys:
    row = [k[0], k[1], str(k[2])]
    for f in files:
        rs = data[f].get(k)
        if not rs: row += ['-'] * 4; continue
        md = lambda x: st.median([r[x] for r in rs])
        row += ['%.1f' % md('db_cpu_us_per_req'), '%.2f' % md('p50_ms'), '%.2f' % md('p99_ms'), str(sum(r['errors'] + r.get('wrong_rows', 0) for r in rs))]
    print('| ' + ' | '.join(row) + ' |')
