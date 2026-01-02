package pds

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
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
	"github.com/jcalabro/atlas/internal/plc"
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

	PLCURL        string
	JWTSigningKey string
	ServiceDID    string

	FDB foundation.Config
}

type server struct {
	log    *slog.Logger
	tracer trace.Tracer

	shutdownOnce sync.Once

	db *foundation.DB

	directory  identity.Directory
	plc        plc.PLC
	signingKey *ecdsa.PrivateKey
	serviceDID string
}

func (s *server) shutdown(cancel context.CancelFunc) {
	s.shutdownOnce.Do(func() {
		s.log.Info("shutdown initiated")
		cancel()
	})
}

func loadSigningKey(path string) (*ecdsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read signing key file: %w", err)
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing signing key")
	}

	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EC private key: %w", err)
	}

	return key, nil
}

func Run(ctx context.Context, args *Args) error {
	log := slog.Default().With(slog.String("service", serviceName))

	log.Info("starting pds server")
	defer log.Info("pds server shutdown complete")

	if err := metrics.InitTracing(ctx, serviceName); err != nil {
		return err
	}
	tracer := otel.Tracer(serviceName)

	plcClient, err := plc.NewClient(&plc.ClientArgs{
		Tracer: tracer,
		PLCURL: args.PLCURL,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize plc client: %w", err)
	}

	signingKey, err := loadSigningKey(args.JWTSigningKey)
	if err != nil {
		return fmt.Errorf("failed to load JWT signing key: %w", err)
	}

	db, err := foundation.New(tracer, args.FDB)
	if err != nil {
		return err
	}

	s := &server{
		log:    log,
		tracer: tracer,
		db:     db,

		// @TODO (jrc): use foundation rather than caching in-memory
		directory:  identity.DefaultDirectory(),
		plc:        plcClient,
		signingKey: signingKey,
		serviceDID: args.ServiceDID,
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

	handler := s.observabilityMiddleware(s.router())

	srv := &http.Server{
		Handler:      handler,
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
	fmt.Fprintf(w, msg, args...)
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

func (s *server) router() *http.ServeMux {
	mux := http.NewServeMux()

	//
	// Misc. routes
	//

	mux.HandleFunc("GET /ping", s.handlePing)
	mux.HandleFunc("GET /xrpc/_health", s.handleHealth)

	//
	// PDS routes
	//

	mux.HandleFunc("GET /xrpc/com.atproto.identity.resolveHandle", s.handleResolveHandle)
	mux.HandleFunc("POST /xrpc/com.atproto.server.createAccount", s.handleCreateAccount)

	return mux
}
