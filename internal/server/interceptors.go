package server

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/internal/env"
	"github.com/jcalabro/atlas/internal/metrics"
)

func loggingInterceptor(log *slog.Logger) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			name := req.Spec().Procedure
			start := time.Now()
			res, err := next(ctx, req)
			elapsedMicros := time.Since(start).Microseconds()
			if err != nil {
				// don't log NotFound errors (they are expected, unimportant, and high frequency)
				connectErr, ok := err.(*connect.Error)
				if ok && connectErr.Code() == connect.CodeNotFound {
					return nil, err
				}

				log.Warn("rpc request failed", "name", name, "elapsed_us", elapsedMicros, "err", err)
				return nil, err
			}

			log.Debug("rpc request succeeded", "name", name, "elapsed_us", elapsedMicros)
			return res, nil
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}

func metricsInterceptor() connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			service, method := serviceAndMethod(req.Spec().Procedure)
			start := time.Now()

			res, err := next(ctx, req)
			elapsedSeconds := time.Since(start).Seconds()
			code := codeText(err)

			metrics.Queries.WithLabelValues(env.Version, service, method, code).Inc()
			metrics.QueryDuration.WithLabelValues(service, method, code).Observe(elapsedSeconds)

			return res, err
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}

// serviceAndMethod returns the service and method from a procedure.
func serviceAndMethod(procedure string) (string, string) {
	procedure = strings.TrimPrefix(procedure, "/")
	service, method := "unknown", "unknown"
	// service should be "bsky" from "bsky.Service/CreateActorProfile"
	if strings.Contains(procedure, "/") {
		long := strings.Split(procedure, "/")[0]
		if strings.Contains(long, ".") {
			service = strings.Split(long, ".")[0]
		}
	}
	// method should be "CreateActorProfile" from "bsky.Service/CreateActorProfile"
	if strings.Contains(procedure, "/") {
		method = strings.Split(procedure, "/")[1]
	}
	return service, method
}

// codeText returns the code text name for an error.
func codeText(err error) string {
	if err == nil {
		return "success"
	}
	connectErr, ok := err.(*connect.Error)
	if !ok {
		return "unknown"
	}
	return connectErr.Code().String()
}
