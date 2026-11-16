package main

import (
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"
)

type hist struct {
	name, help string
	bounds     []float64
	counts     []atomic.Int64
	sumMicro   atomic.Int64 // sum * 1e6
	n          atomic.Int64
}

func newHist(name, help string, bounds []float64) *hist {
	return &hist{name: name, help: help, bounds: bounds, counts: make([]atomic.Int64, len(bounds)+1)}
}

func (h *hist) observe(v float64) {
	i := 0
	for i < len(h.bounds) && v > h.bounds[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumMicro.Add(int64(math.Round(v * 1e6)))
	h.n.Add(1)
}

func (h *hist) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	var cum int64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", h.name, b, cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", h.name, cum, h.name, float64(h.sumMicro.Load())/1e6, h.name, h.n.Load())
}

var (
	stCoalescedReqs, stBatches, stBatchItems, stUnique, stSingles, stFallback, stForwardGroups, stForwardQueries, stLocalGroups, stTmplOK, stTmplNo, stQueueMax                                                                                                                                                                                                                                                   atomic.Int64
	stClients, stClientsTotal, stAuthFail, stCancels, stCancelLocal, stTaints, stServerDials, stRotations, stShutdownKicks, stVerifies, stVerifyMismatch, stTxBegins, stTxCommits, stTxCoalesced, stTxFallbacks, stTxBatches, stTxBatchItems, stEngineSwitches, stTxWatchMismatch, stTxBypass, stEngineCapHits, stEngineIdleClosed, stPanics, stEnginesLive, stBatchTimeouts, stBatchRetries, stAdaptiveImmediate atomic.Int64

	hBatchSize   = newHist("pgcoalesce_batch_size", "Requests per executed batch.", []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
	hBatchKeys   = newHist("pgcoalesce_batch_unique_keys", "Distinct keys per executed batch.", []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
	hWait        = newHist("pgcoalesce_queue_wait_seconds", "Time a coalesced request waited before its batch was sent.", []float64{.0001, .00025, .0005, .001, .002, .003, .005, .01, .025, .05, .1})
	hLatency     = newHist("pgcoalesce_request_seconds", "End-to-end proxy latency of coalesced requests (arrival to result).", []float64{.00025, .0005, .001, .002, .003, .005, .0075, .01, .025, .05, .1, .25})
	hBatchExec   = newHist("pgcoalesce_batch_exec_seconds", "Database round trip per batch.", []float64{.0001, .00025, .0005, .001, .002, .005, .01, .025, .05, .1})
	hTxBatchSize = newHist("pgcoalesce_tx_batch_size", "Client transactions per shared transaction (transaction-template coalescing).", []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
	startTime    = time.Now()
)

func writeMetrics(w io.Writer) {
	c := func(name, help string, v int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	g := func(name, help string, v float64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, v)
	}
	reqs, batches := stCoalescedReqs.Load(), stBatches.Load()
	c("pgcoalesce_coalesced_requests_total", "Point lookups answered through the coalescer.", reqs)
	c("pgcoalesce_batches_total", "Batches sent to Postgres (one statement each).", batches)
	c("pgcoalesce_batch_keys_total", "Distinct keys sent in batches.", stUnique.Load())
	c("pgcoalesce_batch_fallbacks_total", "Batches that failed and were re-run per key to isolate errors.", stFallback.Load())
	c("pgcoalesce_batch_timeouts_total", "Batches canceled after batch_timeout (their requests were re-run individually).", stBatchTimeouts.Load())
	c("pgcoalesce_batch_retried_requests_total", "Requests of a timed-out or timeout-class failed batch re-run individually on the client's own session.", stBatchRetries.Load())
	c("pgcoalesce_adaptive_immediate_total", "Requests dispatched without waiting because fewer than min_expected partners were expected within the window.", stAdaptiveImmediate.Load())
	c("pgcoalesce_forwarded_groups_total", "Client message groups forwarded unchanged to the upstream session.", stForwardGroups.Load())
	c("pgcoalesce_templates_total{result=\"coalescible\"}", "Distinct statements classified.", stTmplOK.Load())
	fmt.Fprintf(w, "pgcoalesce_templates_total{result=\"rejected\"} %d\n", stTmplNo.Load())
	c("pgcoalesce_client_connections_total", "Accepted client connections.", stClientsTotal.Load())
	c("pgcoalesce_auth_failures_total", "Failed client authentications.", stAuthFail.Load())
	c("pgcoalesce_cancel_requests_total", "CancelRequests received.", stCancels.Load())
	c("pgcoalesce_tainted_sessions_total", "Client sessions excluded from coalescing after a session-state change.", stTaints.Load())
	c("pgcoalesce_state_verifications_total", "Session-state fingerprint checks before a client returned to coalescing.", stVerifies.Load())
	c("pgcoalesce_state_mismatches_total", "Checks that found a session state differing from the coalescer baseline (client kept on passthrough).", stVerifyMismatch.Load())
	c("pgcoalesce_server_connections_opened_total", "Coalescer server connections opened.", stServerDials.Load())
	g("pgcoalesce_client_connections", "Connected clients.", float64(stClients.Load()))
	ratio := 0.0
	if batches > 0 {
		ratio = float64(reqs) / float64(batches)
	}
	g("pgcoalesce_coalesce_ratio", "Coalesced requests per database statement since start.", ratio)
	g("pgcoalesce_window_seconds", "Configured max batching window.", cfg.Window.Seconds())
	g("pgcoalesce_queue_max", "Largest pending queue seen.", float64(stQueueMax.Load()))
	g("pgcoalesce_uptime_seconds", "Seconds since start.", time.Since(startTime).Seconds())
	g("pgcoalesce_engines", "Coalescing engines holding server connections (one per distinct session configuration).", float64(stEnginesLive.Load()))
	c("pgcoalesce_engine_limit_hits_total", "Session-settings variants refused because max_engines was reached (client kept on passthrough).", stEngineCapHits.Load())
	c("pgcoalesce_engines_closed_idle_total", "Engines closed because no client used them (idle timeout or to make room).", stEngineIdleClosed.Load())
	c("pgcoalesce_client_panics_total", "Client connections closed after an internal error (bug) instead of crashing the proxy.", stPanics.Load())
	c("pgcoalesce_tx_started_total", "Client transactions entered into transaction-template coalescing.", stTxBegins.Load())
	c("pgcoalesce_tx_coalesced_total", "Client transaction reads answered from a shared transaction.", stTxCoalesced.Load())
	c("pgcoalesce_tx_bypass_total", "Coalescible transactions passed through because their fingerprint is too cold to batch.", stTxBypass.Load())
	c("pgcoalesce_tx_fallbacks_total", "Client transactions replayed on their own session (deviation or error).", stTxFallbacks.Load())
	c("pgcoalesce_tx_batches_total", "Shared transactions executed.", stTxBatches.Load())
	for _, h := range []*hist{hBatchSize, hBatchKeys, hWait, hLatency, hBatchExec, hTxBatchSize} {
		h.write(w)
	}
}

func statsMap() map[string]int64 {
	return map[string]int64{"coalesced_reqs": stCoalescedReqs.Load(), "batches": stBatches.Load(), "batch_items": stBatchItems.Load(), "unique": stUnique.Load(),
		"singles": stSingles.Load(), "fallbacks": stFallback.Load(), "batch_timeouts": stBatchTimeouts.Load(), "batch_retries": stBatchRetries.Load(), "adaptive_immediate": stAdaptiveImmediate.Load(), "forward_groups": stForwardGroups.Load(), "forward_queries": stForwardQueries.Load(),
		"local_groups": stLocalGroups.Load(), "tmpl_ok": stTmplOK.Load(), "tmpl_no": stTmplNo.Load(), "queue_max": stQueueMax.Load(),
		"clients": stClients.Load(), "auth_fail": stAuthFail.Load(), "cancels": stCancels.Load(), "cancel_local": stCancelLocal.Load(), "taints": stTaints.Load(),
		"server_dials": stServerDials.Load(), "rotations": stRotations.Load(), "verifies": stVerifies.Load(), "verify_mismatch": stVerifyMismatch.Load(), "tx_begins": stTxBegins.Load(), "tx_commits": stTxCommits.Load(), "tx_coalesced": stTxCoalesced.Load(), "tx_fallbacks": stTxFallbacks.Load(), "tx_batches": stTxBatches.Load(), "tx_batch_items": stTxBatchItems.Load(), "engine_switches": stEngineSwitches.Load(), "tx_watch_mismatch": stTxWatchMismatch.Load(), "tx_bypass": stTxBypass.Load(),
		"engine_cap_hits": stEngineCapHits.Load(), "engines_closed_idle": stEngineIdleClosed.Load(), "client_panics": stPanics.Load(), "engines": stEnginesLive.Load()}
}
