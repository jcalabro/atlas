package pds

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "atlas_pds"
)

var (
	requests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "requests",
			Namespace: namespace,
			Help:      "Total number of requests served",
		},
		[]string{"version", "service", "host", "handler", "method", "status"},
	)

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "request_duration",
		Namespace: namespace,
		Help:      "Request duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"service", "host", "handler", "method", "status"})
)
