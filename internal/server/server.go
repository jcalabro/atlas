package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "net/http/pprof"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type ServerArgs struct {
	Addr        string
	MetricsAddr string

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

	if err := s.serve(ctx, args.Addr); err != nil {
		return err
	}

	s.log.Info("server shutdown complete")
	return nil
}

func (s *server) serve(ctx context.Context, addr string) error {
	lc := &net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.log.Info("database server listening", "addr", addr)

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	var connWg sync.WaitGroup
	defer connWg.Wait()

	for {
		conn, err := l.Accept()
		if err != nil {
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

	<-ctx.Done()
}
