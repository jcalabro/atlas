package metrics

import (
	"context"
	"fmt"

	"github.com/jcalabro/atlas/internal/env"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func InitTracing(ctx context.Context, service string) error {
	if !env.IsProd() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return fmt.Errorf("failed to create otlp exporter: %w", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return nil
}

func SpanEnd(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "ok")
	} else {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	span.End()
}
