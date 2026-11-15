#!/bin/bash
# start postgres inside cgroup coalesce_pg (1 CPU, 256MB)
echo $$ | sudo tee /sys/fs/cgroup/coalesce_pg/cgroup.procs >/dev/null
exec /workspace/coalesce/pgenv/bin/postgres -D /workspace/coalesce/data
