package server

import (
	"context"
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
)

type ServerArgs struct {
	Addr        string
	TapAddr     string
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

func Run(ctx context.Context, args *ServerArgs) error {
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go metrics.RunServer(ctx, args.MetricsAddr)

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

	wg := &sync.WaitGroup{}

	wg.Go(func() { s.serveGRPC(ctx, args) })
	wg.Go(func() { s.serveTap(ctx, args) })

	s.log.Info("server shutdown complete")
	return nil
}

func (s *server) serveGRPC(ctx context.Context, args *ServerArgs) {
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

func (s *server) serveTap(ctx context.Context, args *ServerArgs) {
}
