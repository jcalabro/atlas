package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/jcalabro/atlas/internal/env"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunServer(ctx context.Context, cancel context.CancelFunc, addr string) {
	if addr == "" {
		slog.Info("metrics server disabled")
		return
	}

	defer cancel()

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/version", env.VersionHandler)
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "OK")
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      nil,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("metrics server listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server error", "err", err)
	}
}
