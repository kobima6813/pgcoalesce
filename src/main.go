package main

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const version = "0.3.0"

func loadAuth() error {
	if cfg.AuthFile == "" {
		return nil
	}
	ul, err := loadUserlist(cfg.AuthFile)
	if err != nil {
		return err
	}
	credMu.Lock()
	userlist = ul
	authCache = map[string]authCacheEntry{}
	credMu.Unlock()
	return nil
}

func loadTLS() error {
	if cfg.ClientTLSMode == "disable" || cfg.ClientTLSMode == "" {
		tlsServerCfg = nil
		return nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.ClientTLSCertFile, cfg.ClientTLSKeyFile)
	if err != nil {
		return err
	}
	tlsServerCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	return nil
}

func main() {
	if err := parseFlags(); err != nil {
		log.Fatal(err)
	}
	// fewer Ps = fewer idle-P spin/wakeups; the proxy is syscall- and wakeup-bound
	if os.Getenv("GOMAXPROCS") == "" {
		n := cfg.Procs
		if n <= 0 {
			n = runtime.NumCPU()
			if n > 4 {
				n = 4
			}
		}
		runtime.GOMAXPROCS(n)
	}
	if err := loadAuth(); err != nil {
		log.Fatalf("auth_file: %v", err)
	}
	if err := loadTLS(); err != nil {
		log.Fatalf("client TLS: %v", err)
	}
	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			writeMetrics(w)
		})
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(statsMap()) })
		mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) { stQueueMax.Store(0); w.Write([]byte("ok")) })
		mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", 405)
				return
			}
			reload()
			w.Write([]byte("ok\n"))
		})
		if cfg.Pprof {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/heap", pprof.Index)
		}
		go func() {
			for { // retry: a previous instance may still be draining
				err := http.ListenAndServe(cfg.MetricsAddr, mux)
				log.Printf("metrics: %v (retrying)", err)
				time.Sleep(time.Second)
			}
		}()
	}
	go engineJanitor()
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("pgcoalesce %s listening on %s; server %s, upstream %s, auth %s, window %v, lanes %d, coalesce %v",
		version, cfg.ListenAddr, cfg.ServerAddr, cfg.UpstreamAddr, cfg.AuthType, cfg.Window, cfg.Lanes, cfg.Coalesce)

	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for s := range sig {
			if s == syscall.SIGHUP {
				reload()
				continue
			}
			shutdown(ln)
		}
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if shuttingDown.Load() {
				select {} // shutdown goroutine exits the process
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go handleClient(c)
	}
}

func reload() {
	if err := loadAuth(); err != nil {
		log.Printf("reload auth_file: %v (keeping old)", err)
	}
	reloadEngines()
	log.Printf("reload: auth reloaded, server connections and templates will be refreshed")
}

// graceful shutdown: stop accepting, disconnect clients as soon as they are idle (not mid-request,
// not inside a transaction), force-close the rest after shutdown_timeout.
func shutdown(ln net.Listener) {
	if shuttingDown.Swap(true) {
		log.Printf("second signal: exiting now")
		os.Exit(1)
	}
	ln.Close()
	log.Printf("shutting down: waiting up to %v for clients to go idle", cfg.ShutdownTimeout)
	deadline := time.Now().Add(cfg.ShutdownTimeout)
	for {
		regMu.Lock()
		var all []*client
		for _, c := range clientsByPID {
			all = append(all, c)
		}
		regMu.Unlock()
		if len(all) == 0 {
			break
		}
		force := time.Now().After(deadline)
		for _, c := range all {
			if force || c.quiescent() {
				stShutdownKicks.Add(1)
				go c.kick()
			}
		}
		if force {
			time.Sleep(200 * time.Millisecond)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("shutdown complete")
	os.Exit(0)
}
