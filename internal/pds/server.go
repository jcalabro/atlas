package pds

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/pds/db"
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

	PLCURL              string
	ConfigFile          string
	FallbackAppviewURLs []string

	FDB db.Config
}

type server struct {
	shutdownOnce sync.Once

	log    *slog.Logger
	tracer trace.Tracer

	hostsMu    sync.RWMutex
	hosts      map[string]*loadedHostConfig
	configFile string

	db        *db.DB
	blobstore *blobstore

	directory    identity.Directory
	plc          plc.PLC
	appviewProxy *appviewProxy
	firehose     *firehose
}

func (s *server) shutdown(cancel context.CancelFunc) {
	s.shutdownOnce.Do(func() {
		s.log.Info("shutdown initiated")
		cancel()
	})
}

func (s *server) reloadConfig() {
	s.log.Info("reloading configuration", "file", s.configFile)

	cfg, err := LoadConfig(s.configFile)
	if err != nil {
		s.log.Error("failed to reload config", "err", err)
		return
	}

	s.hostsMu.Lock()
	s.hosts = cfg.Hosts
	s.hostsMu.Unlock()

	s.log.Info("configuration reloaded successfully", "num_hosts", len(cfg.Hosts))
}

func (s *server) getHost(hostname string) *loadedHostConfig {
	s.hostsMu.RLock()
	defer s.hostsMu.RUnlock()
	return s.hosts[hostname]
}

func Run(ctx context.Context, args *Args) error {
	log := slog.Default().With(slog.String("service", serviceName))

	log.Info("starting pds server")
	defer log.Info("pds server shutdown complete")

	if err := metrics.InitTracing(ctx, serviceName); err != nil {
		return err
	}
	tracer := otel.Tracer(serviceName)

	cfg, err := LoadConfig(args.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	log.Info("loaded host configurations", "hosts", len(cfg.Hosts))

	plcClient, err := plc.NewClient(&plc.ClientArgs{
		Tracer: tracer,
		PLCURL: args.PLCURL,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize plc client: %w", err)
	}

	db, err := db.New(tracer, args.FDB)
	if err != nil {
		return err
	}

	appviewProxy := newAppviewProxy(log, args.FallbackAppviewURLs)
	if appviewProxy != nil {
		log.Info("configured appview proxy", "num_backends", len(args.FallbackAppviewURLs))
	}

	var bs *blobstore
	if cfg.Blobstore != nil {
		bs, err = newBlobstore(cfg.Blobstore)
		if err != nil {
			return fmt.Errorf("failed to initialize blobstore: %w", err)
		}
		log.Info("initialized blobstore", "endpoint", cfg.Blobstore.Endpoint, "bucket", cfg.Blobstore.Bucket)
	}

	s := &server{
		log:    log,
		tracer: tracer,

		hosts:      cfg.Hosts,
		configFile: args.ConfigFile,

		db:        db,
		blobstore: bs,

		// @TODO (jrc): use foundation rather than caching in-memory
		directory: identity.DefaultDirectory(),

		plc:          plcClient,
		appviewProxy: appviewProxy,
		firehose:     newFirehose(log, db),
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
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case received := <-sig:
				if received == syscall.SIGHUP {
					s.reloadConfig()
				} else {
					s.log.Info("received shutdown signal")
					s.shutdown(cancel)
					return
				}
			}
		}
	}()

	errs.Go(func() error {
		metrics.RunServer(ctx, cancel, args.MetricsAddr)
		return nil
	})

	errs.Go(func() error {
		s.appviewProxy.Start(ctx)
		return nil
	})

	errs.Go(func() error {
		s.firehose.Run(ctx)
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

	handler := s.observabilityMiddleware(s.hostMiddleware(s.router()))

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
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
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

func (s *server) unauthorized(w http.ResponseWriter, err error) {
	s.err(w, http.StatusUnauthorized, err)
}

func (s *server) forbidden(w http.ResponseWriter, err error) {
	s.err(w, http.StatusForbidden, err)
}

func (s *server) notFound(w http.ResponseWriter, err error) {
	s.err(w, http.StatusNotFound, err)
}

func (s *server) conflict(w http.ResponseWriter, err error) {
	s.err(w, http.StatusConflict, err)
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

func parseIntParam(r *http.Request, name string, defaultVal int64) (int64, error) {
	str := r.URL.Query().Get(name)
	if str == "" {
		return defaultVal, nil
	}

	return strconv.ParseInt(str, 10, 64)
}

func nextCursorOrNil(cursor string) *string {
	if cursor == "" {
		return nil
	}
	return &cursor
}

func (s *server) router() *http.ServeMux {
	mux := http.NewServeMux()

	//
	// Base routes
	//

	mux.HandleFunc("GET /", s.handleRoot)
	mux.HandleFunc("GET /ping", s.handlePing)
	mux.HandleFunc("GET /robots.txt", s.handleRobots)

	//
	// Well-known routes
	//

	mux.HandleFunc("GET /.well-known/did.json", s.handleWellKnown)
	mux.HandleFunc("GET /.well-known/atproto-did", s.handleAtprotoDid)
	mux.HandleFunc("GET /.well-known/oauth-protected-resource", s.handleOauthProtectedResource)
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", s.handleOauthAuthorizationServer)

	//
	// XRPC routes
	//

	mux.HandleFunc("GET /xrpc/_health", s.handleHealth)

	mux.HandleFunc("GET /xrpc/com.atproto.identity.resolveHandle", s.handleResolveHandle)

	mux.HandleFunc("GET /xrpc/com.atproto.repo.describeRepo", s.handleDescribeRepo)
	mux.HandleFunc("GET /xrpc/com.atproto.repo.getRecord", s.handleGetRecord)
	mux.HandleFunc("POST /xrpc/com.atproto.repo.createRecord", s.authMiddleware(s.handleCreateRecord))
	mux.HandleFunc("POST /xrpc/com.atproto.repo.putRecord", s.authMiddleware(s.handlePutRecord))
	mux.HandleFunc("POST /xrpc/com.atproto.repo.deleteRecord", s.authMiddleware(s.handleDeleteRecord))
	mux.HandleFunc("POST /xrpc/com.atproto.repo.applyWrites", s.authMiddleware(s.handleApplyWrites))
	mux.HandleFunc("POST /xrpc/com.atproto.repo.uploadBlob", s.authMiddleware(s.handleUploadBlob))

	mux.HandleFunc("GET /xrpc/com.atproto.server.describeServer", s.handleDescribeServer)
	mux.HandleFunc("POST /xrpc/com.atproto.server.createAccount", s.handleCreateAccount)
	mux.HandleFunc("POST /xrpc/com.atproto.server.createSession", s.handleCreateSession)
	mux.HandleFunc("GET /xrpc/com.atproto.server.getSession", s.authMiddleware(s.handleGetSession))
	mux.HandleFunc("POST /xrpc/com.atproto.server.refreshSession", s.authMiddleware(s.handleRefreshSession))
	mux.HandleFunc("POST /xrpc/com.atproto.server.deleteSession", s.authMiddleware(s.handleDeleteSession))

	mux.HandleFunc("GET /xrpc/com.atproto.sync.listRepos", s.handleListRepos)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.listBlobs", s.handleListBlobs)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getBlob", s.handleGetBlob)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getBlocks", s.handleGetBlocks)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getRecord", s.handleSyncGetRecord)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getLatestCommit", s.handleGetLatestCommit)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getRepoStatus", s.handleGetRepoStatus)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.getRepo", s.handleGetRepo)
	mux.HandleFunc("GET /xrpc/com.atproto.sync.subscribeRepos", s.handleSubscribeRepos)

	mux.HandleFunc("GET /xrpc/app.bsky.feed.getFeed", s.handleGetFeed)

	mux.HandleFunc("GET /xrpc/app.bsky.actor.getPreferences", s.authMiddleware(s.handleGetPreferences))
	mux.HandleFunc("POST /xrpc/app.bsky.actor.putPreferences", s.authMiddleware(s.handlePutPreferences))

	//
	// Proxy catch-all for unhandled XRPC requests
	//

	mux.HandleFunc("GET /xrpc/", s.handleProxy)
	mux.HandleFunc("POST /xrpc/", s.handleProxy)

	return mux
}
