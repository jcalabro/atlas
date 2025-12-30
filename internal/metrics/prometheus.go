package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	StatusOK    = "ok"
	StatusError = "error"
)

const (
	namespace = "atlas"
)

var (
	Queries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "queries",
			Namespace: namespace,
			Help:      "Total number of queries served",
		},
		[]string{"version", "service", "method", "status"},
	)

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "query_duration_seconds",
		Namespace: namespace,
		Help:      "Query duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"service", "method", "status"})

	IngestMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "ingestions",
		Namespace: namespace,
		Help:      "Total number of messages ingested from Tap",
	}, []string{"version", "action", "status"})

	IngestMessageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "ingestion_duration_seconds",
		Namespace: namespace,
		Help:      "Time to process each ingested message",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"action", "status"})
)
