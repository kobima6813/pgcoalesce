package main

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// fakeServer accepts one coalescer connection and answers each Sync with reply(); a CancelRequest
// on a second connection (matching pid/secret) unblocks a pending reply with 57014.
type fakeServer struct {
	ln       net.Listener
	cancel   chan struct{}
	canceled chan struct{}
}

const fakePID = 4242

var fakeSecret = []byte{9, 8, 7, 6}

func startFake(t *testing.T, reply func(be *pgproto3.Backend, cancel <-chan struct{}) byte) (*fakeServer, *bconn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeServer{ln: ln, cancel: make(chan struct{}, 1), canceled: make(chan struct{}, 1)}
	mainConn := make(chan net.Conn, 1)
	go func() {
		first := true
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if first {
				first = false
				mainConn <- c
				continue
			}
			go func(c net.Conn) { // CancelRequest
				defer c.Close()
				buf := make([]byte, 16)
				if _, err := io.ReadFull(c, buf); err != nil {
					return
				}
				if binary.BigEndian.Uint32(buf[4:]) == 80877102 && binary.BigEndian.Uint32(buf[8:]) == fakePID && string(buf[12:]) == string(fakeSecret) {
					f.cancel <- struct{}{}
					f.canceled <- struct{}{}
				}
			}(c)
		}
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sc := <-mainConn
	go func() {
		be := pgproto3.NewBackend(sc, sc)
		for {
			m, err := be.Receive()
			if err != nil {
				return
			}
			switch m.(type) {
			case *pgproto3.Sync:
				ts := reply(be, f.cancel)
				if ts == 0 {
					ts = 'I'
				}
				be.Send(&pgproto3.ReadyForQuery{TxStatus: ts})
				if be.Flush() != nil {
					return
				}
			case *pgproto3.Query: // ROLLBACK after a failed shared transaction
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if be.Flush() != nil {
					return
				}
			}
		}
	}()
	t.Cleanup(func() { ln.Close(); cc.Close(); sc.Close() })
	b := &bconn{conn: cc, fe: pgproto3.NewFrontend(cc, cc), prepared: map[string]bool{}, params: map[string]string{"client_encoding": "UTF8"},
		pid: fakePID, secret: fakeSecret, addr: ln.Addr().String(), born: time.Now()}
	return f, b
}

func testBatcher(b *bconn) (*batcher, []*req) {
	e := &engine{pool: make(chan *bconn, 1), lanes: 1, batchers: map[string]*batcher{}}
	e.pool <- b
	t := &tmpl{id: 1, ok: true, sql: "SELECT id, name FROM users WHERE id = $1", rewritten: "batch", paramOID: 20}
	bt := &batcher{e: e, t: t, rfmts: []int16{0, 0}}
	var rs []*req
	for _, k := range []string{"1", "2", "1"} {
		rs = append(rs, &req{param: []byte(k), t0: time.Now(), done: make(chan result, 1)})
	}
	return bt, rs
}

func withCfg(t *testing.T, timeout time.Duration) {
	old := cfg
	cfg.ServerTLSMode = "disable"
	cfg.BatchTimeout = timeout
	cfg.ServerLifetime = 0
	t.Cleanup(func() { cfg = old })
}

func TestBatchTimeoutCancelsAndRetriesIndividually(t *testing.T) {
	withCfg(t, 100*time.Millisecond)
	f, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		select { // a batch stuck on a lock: only a cancel ends it
		case <-cancel:
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceling statement due to user request"})
		case <-time.After(10 * time.Second):
		}
		return 0
	})
	bt, rs := testBatcher(b)
	before := stBatchTimeouts.Load()
	t0 := time.Now()
	bt.run(rs)
	el := time.Since(t0)
	for i, r := range rs {
		res := <-r.done
		if !res.retry || res.err != nil || res.rows != nil {
			t.Fatalf("req %d: want retry, got %+v", i, res)
		}
	}
	select {
	case <-f.canceled:
	default:
		t.Fatal("no CancelRequest reached the server")
	}
	if el < 100*time.Millisecond || el > 2*time.Second {
		t.Fatalf("batch took %v, want about batch_timeout", el)
	}
	if !b.broken {
		t.Fatal("connection of a canceled batch must be retired")
	}
	if stBatchTimeouts.Load() != before+1 {
		t.Fatal("batch_timeouts metric not incremented")
	}
}

func TestBatchTimeoutLostCancelStillReturns(t *testing.T) {
	withCfg(t, 50*time.Millisecond)
	old := cancelGrace
	cancelGrace = 300 * time.Millisecond
	t.Cleanup(func() { cancelGrace = old })
	_, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		time.Sleep(3 * time.Second) // ignores the cancel
		return 0
	})
	b.secret = []byte{0, 0, 0, 0} // cancel will not match: lost
	bt, rs := testBatcher(b)
	t0 := time.Now()
	bt.run(rs)
	if el := time.Since(t0); el > 2*time.Second {
		t.Fatalf("lost cancel blocked the batch for %v", el)
	}
	for i, r := range rs {
		if res := <-r.done; !res.retry {
			t.Fatalf("req %d: want retry, got %+v", i, res)
		}
	}
	if !b.broken {
		t.Fatal("connection must be retired")
	}
}

func TestBatchFastPathUnaffected(t *testing.T) {
	withCfg(t, 100*time.Millisecond)
	_, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		be.Send(&pgproto3.ParseComplete{})
		be.Send(&pgproto3.BindComplete{})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("2"), []byte("2"), []byte("bob")}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1"), []byte("1"), []byte("ann")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")})
		return 0
	})
	bt, rs := testBatcher(b)
	before := stBatchTimeouts.Load()
	bt.run(rs)
	want := []string{"ann", "bob", "ann"}
	for i, r := range rs {
		res := <-r.done
		if res.retry || res.err != nil || len(res.rows) != 1 || string(res.rows[0][1]) != want[i] {
			t.Fatalf("req %d: got %+v", i, res)
		}
	}
	time.Sleep(150 * time.Millisecond) // watchdog must have been disarmed
	if b.broken || stBatchTimeouts.Load() != before {
		t.Fatal("fast batch must not time out")
	}
}

func TestServerTimeoutErrorRetriesIndividually(t *testing.T) {
	withCfg(t, 0) // proxy timeout off: the server's statement_timeout fires
	_, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceling statement due to statement timeout"})
		return 0
	})
	bt, rs := testBatcher(b)
	fb := stFallback.Load()
	bt.run(rs)
	for i, r := range rs {
		if res := <-r.done; !res.retry {
			t.Fatalf("req %d: want retry, got %+v", i, res)
		}
	}
	if b.broken {
		t.Fatal("a server-side timeout leaves the connection usable")
	}
	if stFallback.Load() != fb {
		t.Fatal("timeout-class errors must not be re-run key by key on the shared connection")
	}
}

func TestOrdinaryErrorStillIsolatedPerKey(t *testing.T) {
	withCfg(t, time.Second)
	n := 0
	_, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		n++
		if n <= 2 { // the batch fails, then key "1" fails on its own: only its clients see the error
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22P02", Message: "invalid input"})
			return 0
		}
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("x"), []byte("y")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		return 0
	})
	bt, rs := testBatcher(b)
	bt.run(rs)
	for i, r := range rs {
		res := <-r.done
		bad := i != 1 // reqs 0 and 2 asked for key "1"
		if res.retry || (res.err != nil) != bad || (!bad && len(res.rows) != 1) {
			t.Fatalf("req %d: got %+v", i, res)
		}
	}
}

func TestBatchTimeoutConfig(t *testing.T) {
	old := cfg
	t.Cleanup(func() { cfg = old })
	if cfg.BatchTimeout != 500*time.Millisecond {
		t.Fatalf("default batch_timeout = %v", cfg.BatchTimeout)
	}
	for v, want := range map[string]time.Duration{"250ms": 250 * time.Millisecond, "0": 0, "1.5": 1500 * time.Millisecond} {
		if err := setKey("batch_timeout", v); err != nil || cfg.BatchTimeout != want {
			t.Fatalf("batch_timeout=%s -> %v, %v", v, cfg.BatchTimeout, err)
		}
	}
	if setKey("batch_timeout", "-1s") == nil {
		t.Fatal("negative batch_timeout accepted")
	}
}

func TestTimeoutClass(t *testing.T) {
	for _, c := range []string{"57014", "55P03", "40P01", "25P04"} {
		if !timeoutClass(c) {
			t.Fatal(c)
		}
	}
	for _, c := range []string{"22P02", "42P01", "XX000", "23505"} {
		if timeoutClass(c) {
			t.Fatal(c)
		}
	}
}

func TestSelfContained(t *testing.T) {
	g := []pgproto3.FrontendMessage{&pgproto3.Parse{}, &pgproto3.Bind{DestinationPortal: ""}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}}
	if !selfContained(g) {
		t.Fatal("bound in group")
	}
	if selfContained([]pgproto3.FrontendMessage{&pgproto3.Execute{Portal: "p1"}}) {
		t.Fatal("portal from an earlier group")
	}
}

func TestAdaptiveWindowExpected(t *testing.T) {
	old := cfg
	t.Cleanup(func() { cfg = old })
	cfg.Window = 2 * time.Millisecond
	at := func(gap time.Duration, n int) float64 {
		b := &batcher{}
		now := time.Now()
		for i := n; i > 0; i-- {
			b.arr[b.ai%len(b.arr)] = now.Add(-time.Duration(i) * gap)
			b.ai++
		}
		return b.expectedLocked(now)
	}
	if e := at(10*time.Millisecond, 32); e >= 1 { // 100 lookups/s: 0.2 partners per 2 ms
		t.Fatalf("low rate: expected %v, want < 1 (dispatch immediately)", e)
	}
	if e := at(200*time.Microsecond, 32); e < 5 { // 5,000 lookups/s: about 10 per window
		t.Fatalf("high rate: expected %v, want about 10 (hold)", e)
	}
	if e := at(time.Millisecond, 1); e != 0 { // a first request never waits
		t.Fatalf("single arrival: %v", e)
	}
}

func TestTxBatchTimeoutReplaysOnOwnSessions(t *testing.T) {
	withCfg(t, 100*time.Millisecond)
	_, b := startFake(t, func(be *pgproto3.Backend, cancel <-chan struct{}) byte {
		select { // shared transaction blocked until canceled
		case <-cancel:
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceling statement due to user request"})
		case <-time.After(10 * time.Second):
		}
		return 'E'
	})
	e := &engine{pool: make(chan *bconn, 1), lanes: 1}
	e.pool <- b
	tb := &txBatcher{e: e, t: &txTmpl{id: 2, ok: true, sql: "SELECT 1 WHERE $1 IS NOT NULL", paramOID: 20, batchSQL: "batch"}, begin: "BEGIN", rfmts: []int16{0}}
	var rs []*txReq
	for _, k := range []string{"1", "2"} {
		rs = append(rs, &txReq{param: []byte(k), t0: time.Now(), done: make(chan txResult, 1)})
	}
	t0 := time.Now()
	tb.run(rs)
	if el := time.Since(t0); el > 2*time.Second {
		t.Fatalf("shared transaction took %v", el)
	}
	for i, r := range rs {
		if res := <-r.done; !res.fallback {
			t.Fatalf("req %d: want replay on own session, got %+v", i, res)
		}
	}
	if !b.broken {
		t.Fatal("connection must be retired")
	}
}
