package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"connectrpc.com/connect"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/storage"
	"github.com/jcalabro/atlas/pkg/atlas"
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

	store *storage.Store
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
		store:  storage.New(db),
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

	mux := http.NewServeMux()
	path, handler := atlas.NewServiceHandler(s, connect.WithInterceptors(
		loggingInterceptor(s.log),
		metricsInterceptor(),
	))
	mux.Handle(path, handler)

	srv := &http.Server{
		Handler:      mux,
		Addr:         args.Addr,
		ErrorLog:     slog.NewLogLogger(s.log.Handler(), slog.LevelError),
		WriteTimeout: args.WriteTimeout,
		ReadTimeout:  args.ReadTimeout,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.log.Error("shutdown error", "err", err)
		}
	}()

	s.log.Info("server listening", "addr", args.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

func (s *server) Query(ctx context.Context, req *connect.Request[atlas.QueryRequest]) (*connect.Response[atlas.QueryResponse], error) {
	// Currently only support single DID + single collection queries
	if len(req.Msg.Dids) != 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("exactly one DID required"))
	}
	if len(req.Msg.Collections) != 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("exactly one collection required"))
	}

	// Filter and sorts not yet implemented
	if req.Msg.Filter != nil {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("filter not yet implemented"))
	}
	if len(req.Msg.Sorts) > 0 {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("custom sorts not yet implemented"))
	}

	did := req.Msg.Dids[0]
	collection := req.Msg.Collections[0]
	limit := int(req.Msg.Limit)
	cursor := req.Msg.Cursor

	records, nextCursor, err := s.store.ListRecords(did, collection, limit, cursor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &atlas.QueryResponse{
		Records: records,
		Cursor:  nextCursor,
	}

	// Resolve authors if requested
	if req.Msg.Resolve != nil && req.Msg.Resolve.Authors && len(records) > 0 {
		dids := make([]string, 0, len(records))
		seen := make(map[string]bool)
		for _, rec := range records {
			if !seen[rec.Did] {
				seen[rec.Did] = true
				dids = append(dids, rec.Did)
			}
		}

		actors, err := s.store.GetActors(dids)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		resp.Actors = make(map[string]*atlas.Actor, len(actors))
		for _, actor := range actors {
			resp.Actors[actor.Did] = actor
		}
	}

	return connect.NewResponse(resp), nil
}
