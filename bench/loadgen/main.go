package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	target = flag.String("target", "postgres://postgres@127.0.0.1:56432/postgres?sslmode=disable", "")
	direct = flag.String("direct", "postgres://postgres@127.0.0.1:55433/postgres?sslmode=disable", "")
	conns  = flag.Int("conns", 500, "")
	rate   = flag.Int("rate", 5000, "qps")
	dur    = flag.Duration("dur", time.Second, "")
	dist   = flag.String("dist", "uniform", "uniform|zipf")
	mode   = flag.String("mode", "simple", "simple|prepared")
	label  = flag.String("label", "", "")
	cg     = flag.String("cgroup", "/sys/fs/cgroup/coalesce_pg/cpu.stat", "")
)

func cpuUsec() int64 {
	b, _ := os.ReadFile(*cg)
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "usage_usec ") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return -1
}

func loadavg() string { b, _ := os.ReadFile("/proc/loadavg"); return strings.Fields(string(b))[0] }

func main() {
	flag.Parse()
	ctx := context.Background()
	dc, err := pgx.Connect(ctx, *direct)
	if err != nil {
		panic(err)
	}
	cfg, _ := pgx.ParseConfig(*target)
	if *mode == "simple" {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	} else {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	}
	cl := make([]*pgx.Conn, *conns)
	var wgc sync.WaitGroup
	var cerr atomic.Int64
	for i := range cl {
		wgc.Add(1)
		go func(i int) {
			defer wgc.Done()
			c, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				cerr.Add(1)
				return
			}
			cl[i] = c
		}(i)
	}
	wgc.Wait()
	n := int(float64(*rate) * dur.Seconds())
	r := rand.New(rand.NewSource(42))
	z := rand.NewZipf(r, 1.1, 1, 999999)
	ids := make([]int64, n)
	for i := range ids {
		if *dist == "zipf" {
			ids[i] = int64(z.Uint64()) + 1
		} else {
			ids[i] = r.Int63n(1000000) + 1
		}
	}
	uniq := map[int64]bool{}
	for _, v := range ids {
		uniq[v] = true
	}
	// warmup few queries on each conn
	for _, c := range cl {
		if c != nil {
			var x int64
			c.QueryRow(ctx, "SELECT * FROM users WHERE id = 1").Scan(&x, nil, nil, nil, nil, nil)
		}
	}
	time.Sleep(500 * time.Millisecond)
	dc.Exec(ctx, "select pg_stat_statements_reset()")
	time.Sleep(200 * time.Millisecond)
	cpu0 := cpuUsec()
	la0 := loadavg()
	type job struct {
		id    int64
		sched time.Time
	}
	jobs := make(chan job, n)
	lat := make([]float64, n)
	var idx atomic.Int64
	var errs, wrong atomic.Int64
	var wg sync.WaitGroup
	for _, c := range cl {
		if c == nil {
			continue
		}
		wg.Add(1)
		go func(c *pgx.Conn) {
			defer wg.Done()
			for j := range jobs {
				cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				var id int64
				var email, name, plan string
				var ts time.Time
				var meta map[string]any
				err := c.QueryRow(cctx, "SELECT * FROM users WHERE id = "+strconv.FormatInt(j.id, 10)).Scan(&id, &email, &name, &plan, &ts, &meta)
				cancel()
				d := time.Since(j.sched)
				if err != nil {
					errs.Add(1)
				} else if id != j.id || email != fmt.Sprintf("user%d@ex.com", j.id) {
					wrong.Add(1)
				}
				lat[idx.Add(1)-1] = float64(d.Microseconds()) / 1000
			}
		}(c)
	}
	start := time.Now()
	interval := time.Duration(float64(time.Second) / float64(*rate))
	for i := 0; i < n; i++ {
		t := start.Add(time.Duration(i) * interval)
		if w := time.Until(t); w > 0 {
			time.Sleep(w)
		}
		jobs <- job{ids[i], t}
	}
	close(jobs)
	wg.Wait()
	el := time.Since(start)
	time.Sleep(300 * time.Millisecond)
	cpu1 := cpuUsec()
	var calls int64
	var exec float64
	dc.QueryRow(ctx, "select coalesce(sum(calls),0)::bigint, coalesce(sum(total_exec_time),0)::float8 from pg_stat_statements where query ilike '%from users where id%'").Scan(&calls, &exec)
	l := lat[:idx.Load()]
	sort.Float64s(l)
	p := func(q float64) float64 {
		if len(l) == 0 {
			return 0
		}
		return l[int(q*float64(len(l)-1))]
	}
	out := map[string]any{"label": *label, "dist": *dist, "mode": *mode, "conns": *conns, "conn_errors": cerr.Load(), "rate": *rate, "n": n, "unique_ids": len(uniq),
		"elapsed_s": el.Seconds(), "achieved_qps": float64(len(l)) / el.Seconds(), "errors": errs.Load(), "wrong_rows": wrong.Load(),
		"p50_ms": p(0.5), "p90_ms": p(0.9), "p99_ms": p(0.99), "max_ms": p(1),
		"db_cpu_ms": float64(cpu1-cpu0) / 1000, "db_cpu_us_per_req": float64(cpu1-cpu0) / float64(n),
		"pgss_calls": calls, "pgss_exec_ms": exec, "loadavg_before": la0, "loadavg_after": loadavg()}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	for _, c := range cl {
		if c != nil {
			c.Close(ctx)
		}
	}
}
