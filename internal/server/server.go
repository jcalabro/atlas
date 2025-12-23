package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	path, handler := atlas.NewServiceHandler(s)
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

func (s *server) GetRecord(ctx context.Context, req *connect.Request[atlas.GetRecordRequest]) (*connect.Response[atlas.GetRecordResponse], error) {
	var did, collection, rkey string

	if req.Msg.Uri != nil {
		parts, err := parseATURI(*req.Msg.Uri)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		did, collection, rkey = parts[0], parts[1], parts[2]
	} else if req.Msg.Did != nil && req.Msg.Collection != nil && req.Msg.Rkey != nil {
		did, collection, rkey = *req.Msg.Did, *req.Msg.Collection, *req.Msg.Rkey
	} else {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("must provide uri or (did, collection, rkey)"))
	}

	rec, err := s.store.GetRecord(did, collection, rkey)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if rec == nil {
		return connect.NewResponse(&atlas.GetRecordResponse{}), nil
	}

	return connect.NewResponse(&atlas.GetRecordResponse{
		Record: &atlas.Record{
			Uri:        fmt.Sprintf("at://%s/%s/%s", did, collection, rkey),
			Cid:        rec.CID,
			Did:        did,
			Collection: collection,
			Rkey:       rkey,
			Value:      rec.Record,
			IndexedAt:  rec.IndexedAt,
		},
	}), nil
}

func parseATURI(uri string) ([]string, error) {
	if !strings.HasPrefix(uri, "at://") {
		return nil, fmt.Errorf("invalid AT URI: must start with at://")
	}
	rest := strings.TrimPrefix(uri, "at://")
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid AT URI: expected at://did/collection/rkey")
	}
	return parts, nil
}
