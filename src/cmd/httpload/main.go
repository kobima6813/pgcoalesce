// httpload: open-loop HTTP load against PostgREST with Zipf-distributed users.
// Scenarios: s1 anon /products (identical claims), s2 per-user fixed JWT on /profiles (RLS),
// s3 per-request unique JWT (iat/jti differ) on /profiles.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var secret []byte

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
func token(sub int, iat int64, jti string) string {
	c := map[string]any{"sub": strconv.Itoa(sub), "role": "authenticated", "iat": iat, "exp": iat + 3600*24*400}
	if jti != "" {
		c["jti"] = jti
	}
	h := b64([]byte(`{"alg":"HS256","typ":"JWT"}`))
	pb, _ := json.Marshal(c)
	p := b64(pb)
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(h + "." + p))
	return h + "." + p + "." + b64(m.Sum(nil))
}

func cpuOf(pids []string) float64 { // seconds
	t := 0.0
	for _, p := range pids {
		b, err := os.ReadFile("/proc/" + p + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndex(s, ")")+2:])
		u, _ := strconv.ParseFloat(f[11], 64)
		k, _ := strconv.ParseFloat(f[12], 64)
		t += (u + k) / 100
	}
	return t
}
func cgCPU() float64 {
	b, _ := os.ReadFile("/sys/fs/cgroup/coalesce_pg/cpu.stat")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "usage_usec ") {
			v, _ := strconv.ParseFloat(strings.TrimPrefix(l, "usage_usec "), 64)
			return v / 1e6
		}
	}
	return 0
}
func stats(url string) map[string]float64 {
	m := map[string]float64{}
	if url == "" {
		return m
	}
	r, err := http.Get(url)
	if err != nil {
		return m
	}
	defer r.Body.Close()
	json.NewDecoder(r.Body).Decode(&m)
	return m
}

func main() {
	base := flag.String("url", "http://127.0.0.1:3300", "")
	scen := flag.String("s", "s1", "")
	rate := flag.Int("rate", 1000, "")
	dur := flag.Duration("dur", 10*time.Second, "")
	users := flag.Int("users", 2000, "")
	zs := flag.Float64("zipf", 1.1, "")
	nkeys := flag.Int("keys", 100000, "")
	pidsF := flag.String("pids", "", "comma pids (proxy)")
	rpids := flag.String("rpids", "", "comma pids (postgrest)")
	statsURL := flag.String("stats", "", "")
	label := flag.String("label", "", "")
	seed := flag.Int64("seed", 1, "")
	flag.Parse()
	sb, err := os.ReadFile("/workspace/coalesce/secrets/jwt_secret")
	if err != nil {
		panic("secret")
	}
	secret = []byte(strings.TrimSpace(string(sb)))
	userTok := make([]string, *users)
	for i := range userTok {
		userTok[i] = token(i, 1763200000, "")
	}
	tr := &http.Transport{MaxIdleConnsPerHost: 2000, MaxConnsPerHost: 2000, DialContext: (&net.Dialer{}).DialContext, DisableCompression: true}
	cl := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	rng := rand.New(rand.NewSource(*seed))
	zu := rand.NewZipf(rng, *zs, 1, uint64(*users-1))
	var mu sync.Mutex
	var lats []float64
	var errs, bad, nonEmpty int64
	var wg sync.WaitGroup
	pids := strings.Split(*pidsF, ",")
	rp := strings.Split(*rpids, ",")
	// warm-up
	for i := 0; i < 50; i++ {
		r, err := cl.Get(*base + "/products?id=eq.1")
		if err == nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}
	s0, c0, p0, r0 := stats(*statsURL), cgCPU(), cpuOf(pids), cpuOf(rp)
	start := time.Now()
	n := int(float64(*rate) * dur.Seconds())
	iv := time.Second / time.Duration(*rate)
	var seq int64
	for i := 0; i < n; i++ {
		at := start.Add(time.Duration(i) * iv)
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		var path, tok string
		switch *scen {
		case "s1":
			path = "/products?id=eq." + strconv.Itoa(1+rng.Intn(*nkeys))
		default:
			u := int(zu.Uint64())
			// rows owned by u: id = u + 2000*k (ids 1..100000)
			id := u + 2000*(rng.Intn(49)+1)
			if u == 0 {
				id = 2000 * (rng.Intn(50) + 1)
			}
			path = "/profiles?id=eq." + strconv.Itoa(id)
			if *scen == "s2" {
				tok = userTok[u]
			} else {
				tok = token(u, 1763200000+atomic.AddInt64(&seq, 1), strconv.FormatInt(rng.Int63(), 36))
			}
		}
		wg.Add(1)
		go func(path, tok string, at time.Time) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", *base+path, nil)
			req.Header.Set("Accept", "application/json")
			if tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			r, err := cl.Do(req)
			if err != nil {
				atomic.AddInt64(&errs, 1)
				return
			}
			b, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if r.StatusCode != 200 {
				atomic.AddInt64(&bad, 1)
			} else if len(b) > 2 {
				atomic.AddInt64(&nonEmpty, 1)
			}
			l := float64(time.Since(at).Microseconds()) / 1000
			mu.Lock()
			lats = append(lats, l)
			mu.Unlock()
		}(path, tok, at)
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	s1, c1, p1, r1 := stats(*statsURL), cgCPU(), cpuOf(pids), cpuOf(rp)
	sort.Float64s(lats)
	pct := func(q float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(q*float64(len(lats)-1))]
	}
	out := map[string]any{"label": *label, "scenario": *scen, "rate": *rate, "n": n, "elapsed_s": el, "ok": len(lats), "errors": errs, "non200": bad, "nonempty": nonEmpty,
		"p50_ms": pct(.5), "p99_ms": pct(.99), "db_cpu_us_per_req": (c1 - c0) * 1e6 / float64(n),
		"proxy_cpu_us_per_req": (p1 - p0) * 1e6 / float64(n), "postgrest_cpu_us_per_req": (r1 - r0) * 1e6 / float64(n)}
	for _, k := range []string{"tx_begins", "tx_coalesced", "tx_batches", "tx_batch_items", "tx_fallbacks", "tx_watch_mismatch", "coalesced_reqs", "forward_groups", "verifies"} {
		out[k] = s1[k] - s0[k]
	}
	j, _ := json.Marshal(out)
	fmt.Println(string(j))
}
