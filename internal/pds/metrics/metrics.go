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

	// Blob storage metrics
	BlobUploads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "blob_uploads_total",
			Namespace: namespace,
			Help:      "Total number of blob uploads",
		},
		[]string{"status"},
	)

	BlobUploadBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name:      "blob_upload_bytes_total",
			Namespace: namespace,
			Help:      "Total bytes uploaded to blob storage",
		},
	)

	BlobDownloads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "blob_downloads_total",
			Namespace: namespace,
			Help:      "Total number of blob downloads",
		},
		[]string{"status"},
	)

	BlobDownloadBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name:      "blob_download_bytes_total",
			Namespace: namespace,
			Help:      "Total bytes downloaded from blob storage",
		},
	)

	// Proxy metrics
	ProxyRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "proxy_requests_total",
			Namespace: namespace,
			Help:      "Total number of proxied requests to appview",
		},
		[]string{"method", "status"},
	)

	ProxyDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:      "proxy_duration_seconds",
			Namespace: namespace,
			Help:      "Duration of proxied requests to appview",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 15), // 10ms to ~5min
		},
		[]string{"method"},
	)

	ProxyErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "proxy_errors_total",
			Namespace: namespace,
			Help:      "Total number of proxy errors",
		},
		[]string{"error_type"},
	)

	// Authentication metrics
	AuthAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "auth_attempts_total",
			Namespace: namespace,
			Help:      "Total number of authentication attempts",
		},
		[]string{"type", "status"}, // type: login, refresh; status: success, failure
	)

	AccountCreations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "account_creations_total",
			Namespace: namespace,
			Help:      "Total number of account creation attempts",
		},
		[]string{"status"},
	)

	// Record operation metrics
	RecordOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:      "record_operations_total",
			Namespace: namespace,
			Help:      "Total number of record operations",
		},
		[]string{"operation", "collection", "status"}, // operation: create, update, delete
	)
)
