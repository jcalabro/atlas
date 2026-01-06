package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "atlas_pds"
)

var (
	Requests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "requests",
			Namespace: namespace,
			Help:      "Total number of requests served",
		},
		[]string{"version", "service", "host", "handler", "method", "status"},
	)

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "request_duration",
		Namespace: namespace,
		Help:      "Request duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"service", "host", "handler", "method", "status"})

	Queries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "queries",
			Namespace: namespace,
			Help:      "Total number of FDB queries",
		},
		[]string{"query", "status"},
	)

	QueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      "query_duration_seconds",
			Namespace: namespace,
			Help:      "Duration histogram of FDB queries in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 18), // 0.1ms to ~13s
		},
		[]string{"query", "status"},
	)

	// Firehose metrics
	FirehoseSubscribers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:      "firehose_subscribers",
			Namespace: namespace,
			Help:      "Current number of firehose subscribers",
		},
		[]string{"pds_host"},
	)

	FirehoseEventsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "firehose_events_sent",
			Namespace: namespace,
			Help:      "Total number of events sent to firehose subscribers",
		},
		[]string{"pds_host"},
	)

	FirehoseEventsDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "firehose_events_dropped",
			Namespace: namespace,
			Help:      "Total number of events dropped due to slow subscribers",
		},
		[]string{"pds_host"},
	)
)
