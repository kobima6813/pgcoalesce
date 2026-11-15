"""wrap.py <metrics_base_url> <label> -- cmd...   snapshots /metrics + /stats around a suite run"""
import sys, json, subprocess, urllib.request, re, time
base, label = sys.argv[1], sys.argv[2]; cmd = sys.argv[sys.argv.index('--')+1:]
def snap():
    m = urllib.request.urlopen(base + '/metrics').read().decode()
    d = {}
    for h in ('pgcoalesce_batch_size', 'pgcoalesce_batch_unique_keys', 'pgcoalesce_tx_batch_size'):
        le1 = re.search(r'^%s_bucket\{le="1"\} (\d+)' % h, m, re.M); cnt = re.search(r'^%s_count (\d+)' % h, m, re.M)
        d[h] = (int(le1.group(1)), int(cnt.group(1)))
    s = json.loads(urllib.request.urlopen(base + '/stats').read())
    return d, s
a = snap(); t0 = time.time()
p = subprocess.run(cmd, capture_output=True, text=True)
b = snap()
out = {'label': label, 'rc': p.returncode, 'secs': round(time.time()-t0, 1)}
for h in a[0]:
    n = b[0][h][1]-a[0][h][1]; one = b[0][h][0]-a[0][h][0]
    out[h] = {'batches': n, 'gt1': n-one}
for k in ('batch_items', 'batches', 'tx_batches', 'tx_batch_items', 'tx_coalesced', 'tx_fallbacks', 'verify_mismatch', 'client_panics', 'engines', 'engine_cap_hits'):
    if k in b[1]: out['stats_'+k] = b[1][k] - a[1].get(k, 0) if k not in ('engines',) else b[1][k]
print(json.dumps(out))
sys.stdout.write('\n--- stdout tail ---\n' + p.stdout[-6000:] + '\n--- stderr tail ---\n' + p.stderr[-3000:])
