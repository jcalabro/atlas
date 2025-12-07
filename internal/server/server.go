package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/env"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Args struct {
	ServerAddr  string
	MetricsAddr string

	TapAddr string

	FDBClusterFile           string
	FDBAPIVersion            int
	FDBTransactionTimeout    int64
	FDBTransactionRetryLimit int64
}

type server struct {
	log    *slog.Logger
	tracer trace.Tracer

	shutOnce sync.Once

	fdb fdb.Database
}

func (s *server) shutdown(cancel context.CancelFunc) {
	s.shutOnce.Do(func() {
		s.log.Info("shutdown initiated")
		cancel()
	})
}

func newServer(ctx context.Context, args *Args) (*server, error) {
	// initialize tracing
	if !env.IsProd() {
		otel.SetTracerProvider(noop.NewTracerProvider())
	} else {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp exporter: %w", err)
		}
		tp := tracesdk.NewTracerProvider(
			tracesdk.WithBatcher(exp),
			tracesdk.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("atlas"),
			)),
		)
		otel.SetTracerProvider(tp)
		tc := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
		otel.SetTextMapPropagator(tc)
	}

	s := &server{
		log:    slog.Default().With(slog.String("group", "server")),
		tracer: otel.Tracer("atlas"),
	}

	// initialize the foundation client
	err := fdb.APIVersion(730)
	if err != nil {
		return nil, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	s.fdb, err = fdb.OpenDatabase(args.FDBClusterFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", args.FDBClusterFile, err)
	}

	// send a ping to ensure we are configured correctly
	_, err = s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to ping foundationdb: %w", err)
	}

	if err := s.fdb.Options().SetTransactionTimeout(args.FDBTransactionTimeout); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}
	if err := s.fdb.Options().SetTransactionRetryLimit(args.FDBTransactionRetryLimit); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	return s, nil
}

func Run(ctx context.Context, args *Args) error {
	s, err := newServer(ctx, args)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.metricsServer(ctx, args)

	// listen for shutdown signals to gracefully stop the system
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-ctx.Done():
		case <-sig:
			s.log.Info("received shutdown signal")
			s.shutdown(cancel)
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.serve(ctx, args); err != nil {
			s.log.Error("database server error", "err", err)
		}
		s.shutdown(cancel)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.ingest(ctx, args); err != nil {
			s.log.Error("ingester error", "err", err)
		}
		s.shutdown(cancel)
	}()

	wg.Wait()
	s.log.Info("server shutdown complete")

	return nil
}

func (s *server) metricsServer(ctx context.Context, args *Args) {
	if args.MetricsAddr == "" {
		s.log.Info("metrics server disabled")
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/version", env.VersionHandler)
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "OK")
	})

	srv := &http.Server{
		Addr:         args.MetricsAddr,
		Handler:      mux,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	s.log.Info("metrics server listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error("metrics server error", "err", err)
	}
}

func (s *server) serve(ctx context.Context, args *Args) error {
	lc := &net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", args.ServerAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", args.ServerAddr, err)
	}

	s.log.Info("database server listening", "addr", args.ServerAddr)

	// shutdown when complete
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	// Track active connections for graceful shutdown
	var connWg sync.WaitGroup
	defer connWg.Wait()

	for {
		conn, err := l.Accept()
		if err != nil {
			// Check if this is due to shutdown
			if ctx.Err() != nil {
				s.log.Info("database server stopped")
				return nil
			}
			s.log.Warn("failed to accept connection", "err", err)
			continue
		}

		connWg.Add(1)
		go func() {
			defer connWg.Done()
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *server) handleConn(ctx context.Context, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.log.Warn("failed to close client connection", "err", err)
		}
	}()

	// @TODO (jrc): implement session handling

	// for now, just wait for context cancellation to simulate a long-running session
	<-ctx.Done()
}
