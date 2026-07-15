package server

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	kvRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "distkv_kv_requests_total",
		Help: "Total KV RPC requests served.",
	}, []string{"op"})

	kvErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "distkv_kv_errors_total",
		Help: "Total KV RPC errors.",
	}, []string{"op"})

	kvLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "distkv_kv_request_duration_seconds",
		Help:    "KV RPC latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms .. ~4s
	}, []string{"op"})

	readIndexLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "distkv_readindex_duration_seconds",
		Help:    "ReadIndex quorum confirmation latency.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})

	proposalsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "distkv_proposals_total",
		Help: "Total write proposals submitted to Raft.",
	})

	leaderElections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "distkv_leader_elections_total",
		Help: "Times this node became Raft leader.",
	})

	isLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_is_leader",
		Help: "1 if this node is the current Raft leader.",
	})

	raftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_raft_term",
		Help: "Current Raft term.",
	})

	commitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_commit_index",
		Help: "Current Raft commit index.",
	})

	appliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_applied_index",
		Help: "Highest index applied to the state machine.",
	})

	applyLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_apply_lag",
		Help: "commit_index - applied_index.",
	})

	leaderID = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "distkv_leader_id",
		Help: "ID of the current Raft leader (0 if unknown).",
	})
)

func observeKV(op string, start time.Time, err error) {
	kvRequests.WithLabelValues(op).Inc()
	kvLatency.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err != nil {
		kvErrors.WithLabelValues(op).Inc()
	}
}

// runMetricsCollector periodically publishes Raft/state-machine gauges.
func (s *Server) runMetricsCollector(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var wasLeader bool
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			term, isL, lid := s.node.Status()
			commit := s.node.CommitIndex()
			s.mu.Lock()
			applied := s.applied
			s.mu.Unlock()

			if isL && !wasLeader {
				leaderElections.Inc()
			}
			wasLeader = isL

			if isL {
				isLeader.Set(1)
			} else {
				isLeader.Set(0)
			}
			raftTerm.Set(float64(term))
			commitIndex.Set(float64(commit))
			appliedIndex.Set(float64(applied))
			if commit >= applied {
				applyLag.Set(float64(commit - applied))
			} else {
				applyLag.Set(0)
			}
			leaderID.Set(float64(lid))
		}
	}
}

func metricsHandler() http.Handler {
	return promhttp.Handler()
}
