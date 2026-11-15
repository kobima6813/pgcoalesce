#!/bin/bash
# percpu.sh <name> <port> <proxypid>: run 1000/s 4s; report CPU ticks of PG backends grouped by owner (old engine / fixed engine / other) + postmaster
cd /workspace/coalesce; ulimit -n 65535
snap(){ for p in $(cat /sys/fs/cgroup/coalesce_pg/cgroup.procs); do [ -r /proc/$p/stat ] && echo "$p $(awk '{print $14+$15}' /proc/$p/stat)"; done; }
owner(){ # map PG backend pid -> owning proxy pid via local port
 ss -tnpH state established 2>/dev/null | awk '$4 ~ /:55433$/ {print $5, $6}' ; }
A=$(snap); ./loadgen2/loadgen2 -target "postgres://postgres@127.0.0.1:$2/postgres?sslmode=disable" -pids $3 -mode prepared -conns 500 -rate 1000 -dur 4s -dist uniform -label "pc_$1" > /tmp/pc_$1.json 2>/dev/null; B=$(snap)
python3 - "$A" "$B" "$1" <<'PY'
import sys, subprocess, re
a=dict(l.split() for l in sys.argv[1].splitlines()); b=dict(l.split() for l in sys.argv[2].splitlines())
# backend pid -> client port from pg_stat_activity
out=subprocess.run(['/workspace/coalesce/pgenv/bin/psql','-h','127.0.0.1','-p','55433','-U','postgres','-AtF',' ','-c',"select pid, coalesce(client_port,0), backend_type from pg_stat_activity"],capture_output=True,text=True).stdout
port={}; bt={}
for l in out.splitlines():
    p,cp,t=l.split(' ',2); port[p]=cp; bt[p]=t
ss=subprocess.run(['ss','-tnpH'],capture_output=True,text=True).stdout
own={}
for l in ss.splitlines():
    f=l.split()
    if len(f)<6: continue
    m=re.search(r'pid=(\d+)',f[-1]) if 'users' in f[-1] else None
    if f[4].endswith(':55433') and m: own[f[3].rsplit(':',1)[1]]=m.group(1)
tot={}
new=0
for p in b:
    d=int(b[p])-int(a.get(p,0))
    if p not in a: new+=d
    o=own.get(port.get(p,''),'?'); key=(bt.get(p,'exited/new'), o)
    tot[key]=tot.get(key,0)+d
print(sys.argv[3], 'total_ticks', sum(int(b[p])-int(a.get(p,0)) for p in b), 'new_procs_ticks', new, 'procs_new', len(set(b)-set(a)), 'procs_gone', len(set(a)-set(b)))
for k,v in sorted(tot.items(), key=lambda x:-x[1])[:8]: print('  ', k, v)
PY
python3 -c "import json;r=json.load(open('/tmp/pc_$1.json'));print('  db_us',r['db_cpu_us_per_req'])" 2>/dev/null || tail -c 300 /tmp/pc_$1.json
