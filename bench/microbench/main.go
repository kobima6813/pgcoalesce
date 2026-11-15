package main

import (
	"flag"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func cpu() int64 {
	b, _ := os.ReadFile("/sys/fs/cgroup/coalesce_pg/cpu.stat")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "usage_usec ") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return 0
}

var maxid = flag.Int("maxid", 1000000, "")
func main() {
	flag.Parse()
	ctx := context.Background()
	c, _ := pgx.Connect(ctx, "postgres://postgres@127.0.0.1:55433/postgres?sslmode=disable")
	forms := map[string]string{
		"join_ord": "SELECT __cq.__cq_o, users.* FROM unnest($1::pg_catalog.int4[]) WITH ORDINALITY AS __cq(__cq_v, __cq_o) JOIN public.users ON users.id = __cq.__cq_v",
		"any":      "SELECT users.id, users.* FROM public.users WHERE id = ANY($1::pg_catalog.int4[])",
		"single":   "SELECT * FROM public.users WHERE id = $1",
		"any_pos":  "SELECT array_positions($1::pg_catalog.int4[], users.id), users.* FROM public.users WHERE id = ANY($1::pg_catalog.int4[])",
	}
	r := rand.New(rand.NewSource(1))
	for _, n := range []int{1, 10, 30, 100} {
		for _, f := range []string{"single", "join_ord", "any", "any_pos"} {
			iters := 3000 / n
			if f == "single" {
				iters = 3000
			}
			c0 := cpu()
			t0 := time.Now()
			for i := 0; i < iters; i++ {
				if f == "single" {
					rows, _ := c.Query(ctx, forms[f], int32(r.Intn(*maxid)+1))
					for rows.Next() {
						rows.RawValues()
					}
					rows.Close()
					continue
				}
				ids := make([]int32, n)
				for j := range ids {
					ids[j] = int32(r.Intn(*maxid) + 1)
				}
				rows, err := c.Query(ctx, forms[f], ids)
				if err != nil {
					panic(err)
				}
				for rows.Next() {
					rows.RawValues()
				}
				rows.Close()
			}
			d := cpu() - c0
			if f == "single" {
				fmt.Printf("single: %.1f us/key DB CPU (%.0f us wall/q)\n", float64(d)/3000, float64(time.Since(t0).Microseconds())/3000)
				continue
			}
			fmt.Printf("n=%d %s: %.1f us/key, %.0f us/batch DB CPU\n", n, f, float64(d)/float64(iters*n), float64(d)/float64(iters))
		}
	}
}
