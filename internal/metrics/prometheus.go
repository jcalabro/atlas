package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
		[]string{"cmd", "status"},
	)

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "query_duration_seconds",
		Namespace: namespace,
		Help:      "Query duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
	}, []string{"cmd", "status"})

	IngestMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "ingest_messages_total",
		Namespace: namespace,
		Help:      "Total number of messages ingested from Tap",
	}, []string{"action", "status"})

	IngestMessageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "ingest_message_duration_seconds",
		Namespace: namespace,
		Help:      "Time to process each ingested message",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"status"})
)

func SpanEnd(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "ok")
	} else {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	span.End()
}
