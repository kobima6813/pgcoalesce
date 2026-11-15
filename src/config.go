package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr   string
	ServerAddr   string // Postgres used for coalesced reads (and auth_query)
	UpstreamAddr string // passthrough target: Postgres or PgBouncer (default: ServerAddr)

	ServerTLSMode       string // disable|prefer|require|verify-ca|verify-full
	ServerTLSCAFile     string
	ServerTLSServerName string
	ClientTLSMode       string // disable|allow|require
	ClientTLSCertFile   string
	ClientTLSKeyFile    string

	AuthType  string // trust|md5|scram-sha-256
	AuthFile  string
	AuthUser  string
	AuthDB    string
	AuthQuery string

	Coalesce       bool
	TxCoalesce     bool
	Procs          int
	Lanes          int
	Window         time.Duration
	MinExpected    float64
	MaxBatch       int
	PosMax         int
	ServerLifetime time.Duration
	TemplateTTL    time.Duration
	MaxEngines     int           // upper bound on coalescing engines (each holds `lanes` server connections); 0 = unlimited
	EngineIdle     time.Duration // close an engine's server connections after this long without clients; 0 = never

	ShutdownTimeout time.Duration
	MetricsAddr     string
	Pprof           bool
}

var cfg = Config{
	ListenAddr: "127.0.0.1:6432", ServerAddr: "127.0.0.1:5432",
	ServerTLSMode: "prefer", ClientTLSMode: "disable",
	AuthType: "scram-sha-256", AuthDB: "postgres",
	AuthQuery: "SELECT usename, passwd FROM pg_catalog.pg_shadow WHERE usename = $1",
	Coalesce:  true, TxCoalesce: true, Lanes: 4, Window: 2 * time.Millisecond, MinExpected: 1.0, MaxBatch: 1000, PosMax: 64,
	ServerLifetime: time.Hour, TemplateTTL: 5 * time.Minute, ShutdownTimeout: 30 * time.Second,
	MaxEngines: 32, EngineIdle: time.Minute,
	MetricsAddr: "127.0.0.1:9930",
}

// INI file, PgBouncer style: a [pgcoalesce] section of key = value lines; ';' or '#' comments.
func loadConfig(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	ln := 0
	for sc.Scan() {
		ln++
		l := strings.TrimSpace(sc.Text())
		if l == "" || l[0] == ';' || l[0] == '#' || l[0] == '[' {
			continue
		}
		k, v, ok := strings.Cut(l, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected key = value", path, ln)
		}
		if err := setKey(strings.TrimSpace(k), strings.TrimSpace(v)); err != nil {
			return fmt.Errorf("%s:%d: %v", path, ln, err)
		}
	}
	return sc.Err()
}

func parseDur(v string) (time.Duration, error) {
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(n * float64(time.Second)), nil // bare number = seconds (PgBouncer style)
	}
	return time.ParseDuration(v)
}

func parseBool(v string) (bool, error) {
	switch strings.ToLower(v) {
	case "1", "on", "yes", "true":
		return true, nil
	case "0", "off", "no", "false":
		return false, nil
	}
	return false, fmt.Errorf("bad boolean %q", v)
}

func setKey(k, v string) error {
	var err error
	switch k {
	case "listen_addr":
		cfg.ListenAddr = v
	case "server_addr":
		cfg.ServerAddr = v
	case "upstream_addr":
		cfg.UpstreamAddr = v
	case "server_tls_sslmode":
		cfg.ServerTLSMode = v
	case "server_tls_ca_file":
		cfg.ServerTLSCAFile = v
	case "server_tls_server_name":
		cfg.ServerTLSServerName = v
	case "client_tls_sslmode":
		cfg.ClientTLSMode = v
	case "client_tls_cert_file":
		cfg.ClientTLSCertFile = v
	case "client_tls_key_file":
		cfg.ClientTLSKeyFile = v
	case "auth_type":
		cfg.AuthType = v
	case "auth_file":
		cfg.AuthFile = v
	case "auth_user":
		cfg.AuthUser = v
	case "auth_dbname":
		cfg.AuthDB = v
	case "auth_query":
		cfg.AuthQuery = v
	case "coalesce":
		cfg.Coalesce, err = parseBool(v)
	case "tx_coalesce":
		cfg.TxCoalesce, err = parseBool(v)
	case "procs":
		cfg.Procs, err = strconv.Atoi(v)
	case "lanes":
		cfg.Lanes, err = strconv.Atoi(v)
	case "window":
		cfg.Window, err = parseDur(v)
	case "min_expected":
		cfg.MinExpected, err = strconv.ParseFloat(v, 64)
	case "max_batch":
		cfg.MaxBatch, err = strconv.Atoi(v)
	case "pos_max":
		cfg.PosMax, err = strconv.Atoi(v)
	case "server_lifetime":
		cfg.ServerLifetime, err = parseDur(v)
	case "template_ttl":
		cfg.TemplateTTL, err = parseDur(v)
	case "max_engines":
		cfg.MaxEngines, err = strconv.Atoi(v)
	case "engine_idle_timeout":
		cfg.EngineIdle, err = parseDur(v)
	case "shutdown_timeout":
		cfg.ShutdownTimeout, err = parseDur(v)
	case "metrics_addr":
		cfg.MetricsAddr = v
	case "pprof":
		cfg.Pprof, err = parseBool(v)
	default:
		return fmt.Errorf("unknown setting %q", k)
	}
	return err
}

// flags: -config FILE, plus "-set key=value" (repeatable) and a few short aliases used by the benchmark scripts.
type setFlags []string

func (s *setFlags) String() string     { return strings.Join(*s, ",") }
func (s *setFlags) Set(v string) error { *s = append(*s, v); return nil }

func parseFlags() error {
	path := flag.String("config", "", "config file (INI)")
	var sets setFlags
	flag.Var(&sets, "set", "override a setting: -set key=value (repeatable)")
	aliases := map[string]*string{}
	for _, a := range [][2]string{{"listen", "listen_addr"}, {"server", "server_addr"}, {"upstream", "upstream_addr"}, {"window", "window"}, {"lanes", "lanes"}, {"metrics", "metrics_addr"}, {"minexp", "min_expected"}, {"posmax", "pos_max"}, {"maxbatch", "max_batch"}} {
		aliases[a[1]] = flag.String(a[0], "", "alias for "+a[1])
	}
	passthrough := flag.Bool("passthrough", false, "alias for coalesce=off")
	flag.Parse()
	if *path != "" {
		if err := loadConfig(*path); err != nil {
			return err
		}
	}
	for k, p := range aliases {
		if *p != "" {
			if err := setKey(k, *p); err != nil {
				return err
			}
		}
	}
	if *passthrough {
		cfg.Coalesce = false
	}
	for _, s := range sets {
		k, v, _ := strings.Cut(s, "=")
		if err := setKey(strings.TrimSpace(k), strings.TrimSpace(v)); err != nil {
			return err
		}
	}
	if cfg.UpstreamAddr == "" {
		cfg.UpstreamAddr = cfg.ServerAddr
	}
	switch cfg.AuthType {
	case "trust", "md5", "scram-sha-256":
	default:
		return fmt.Errorf("auth_type must be trust, md5 or scram-sha-256")
	}
	if cfg.Lanes < 1 {
		cfg.Lanes = 1
	}
	return nil
}
