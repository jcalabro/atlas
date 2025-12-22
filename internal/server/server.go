package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type Args struct {
	Addr        string
	MetricsAddr string

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	FDB foundation.Config
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

func Run(ctx context.Context, args *Args) error {
	if err := metrics.InitTracing(ctx, "atlas.server"); err != nil {
		return err
	}

	db, err := foundation.Open(args.FDB)
	if err != nil {
		return err
	}

	s := &server{
		log:    slog.Default().With(slog.String("component", "server")),
		tracer: otel.Tracer("atlas.server"),
		fdb:    db,
	}

	cancelOnce := &sync.Once{}
	ctx, cancelFn := context.WithCancel(ctx)
	cancel := func() {
		cancelOnce.Do(func() {
			cancelFn()
		})
	}
	defer cancel()

	errs, ctx := errgroup.WithContext(ctx)

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

	errs.Go(func() error {
		metrics.RunServer(ctx, cancel, args.MetricsAddr)
		return nil
	})

	errs.Go(func() error {
		if err := s.serve(ctx, cancel, args); err != nil {
			return fmt.Errorf("failed to run connect rpc server: %w", err)
		}

		s.log.Info("server shutdown complete")
		return nil
	})

	return errs.Wait()
}

func (s *server) serve(ctx context.Context, cancel context.CancelFunc, args *Args) error {
	defer cancel()

	return nil

	// srv := &http.Server{
	// 	Handler:        mux,
	// 	Addr:           args.Addr,
	// 	ErrorLog:       slog.NewLogLogger(s.log.Handler(), slog.LevelError),
	// 	WriteTimeout:   args.Timeout,
	// 	ReadTimeout:    args.Timeout,
	// 	MaxHeaderBytes: httpMaxHeaderBytes,
	// 	TLSConfig: &tls.Config{
	// 		NextProtos:   []string{"h2"}, // HTTP2 *only*.
	// 		MinVersion:   tls.VersionTLS13,
	// 		Certificates: []tls.Certificate{*certificate},
	// 	},
	// }
	// if err := http2.ConfigureServer(s.grpcServer, &http2.Server{
	// 	MaxConcurrentStreams: 100_000,
	// 	MaxHandlers:          1_000_000, // Not actually implemented?
	// }); err != nil {
	// 	return nil, fmt.Errorf("failed to configure HTTP2 server: %w", err)
	// }
}
