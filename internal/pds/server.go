package pds

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	serviceName = "atlas.pds"
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

	shutdownOnce sync.Once

	db *foundation.DB

	directory identity.Directory
}

func (s *server) shutdown(cancel context.CancelFunc) {
	s.shutdownOnce.Do(func() {
		s.log.Info("shutdown initiated")
		cancel()
	})
}

func Run(ctx context.Context, args *Args) error {
	log := slog.Default().With(slog.String("service", serviceName))

	log.Info("starting pds server")
	defer log.Info("pds server shutdown complete")

	if err := metrics.InitTracing(ctx, serviceName); err != nil {
		return err
	}
	tracer := otel.Tracer(serviceName)

	db, err := foundation.New(tracer, args.FDB)
	if err != nil {
		return err
	}

	s := &server{
		log:    log,
		tracer: tracer,
		db:     db,

		// @TODO (jrc): use foundation rather than caching in-memory
		directory: identity.DefaultDirectory(),
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

		return nil
	})

	return errs.Wait()
}

func (s *server) serve(ctx context.Context, cancel context.CancelFunc, args *Args) error {
	defer cancel()

	srv := &http.Server{
		Handler:      s.router(),
		Addr:         args.Addr,
		ErrorLog:     slog.NewLogLogger(s.log.Handler(), slog.LevelError),
		WriteTimeout: args.WriteTimeout,
		ReadTimeout:  args.ReadTimeout,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		srv.SetKeepAlivesEnabled(false)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.log.Error("server shutdown error", "err", err)
		}
	}()

	s.log.Info("server listening", "addr", args.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

func (s *server) plaintextOK(w http.ResponseWriter, msg string, args ...any) {
	s.plaintextWithCode(w, http.StatusOK, msg, args...)
}

func (s *server) plaintextWithCode(w http.ResponseWriter, code int, msg string, args ...any) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	fmt.Fprintf(w, msg, args...) // nolint:errcheck
}

func (s *server) jsonOK(w http.ResponseWriter, resp any) {
	s.jsonWithCode(w, http.StatusOK, resp)
}

func (s *server) jsonWithCode(w http.ResponseWriter, code int, resp any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Error("failed to json encode and write repsonse", "err", err)
		return
	}
}

func (s *server) badRequest(w http.ResponseWriter, err error) {
	s.err(w, http.StatusBadRequest, err)
}

func (s *server) notFound(w http.ResponseWriter, err error) {
	s.err(w, http.StatusNotFound, err)
}

func (s *server) internalErr(w http.ResponseWriter, err error) {
	s.err(w, http.StatusInternalServerError, err)
}

func (s *server) err(w http.ResponseWriter, code int, err error) {
	type response struct {
		Err string `json:"msg"`
	}

	s.jsonWithCode(w, code, &response{
		Err: err.Error(),
	})
}

func (s *server) handleFunc(mux *http.ServeMux, pattern string, fn observableHandlerFunc) {
	mux.HandleFunc(pattern, s.observabilityMiddleware(fn))
}

func (s *server) router() *http.ServeMux {
	mux := http.NewServeMux()

	//
	// Misc. routes
	//

	s.handleFunc(mux, "GET /ping", handlePing)
	s.handleFunc(mux, "GET /xrpc/_health", handleHealth)

	//
	// PDS routes
	//

	s.handleFunc(mux, "GET /xrpc/com.atproto.identity.resolveHandle", handleResolveHandle)

	return mux
}
