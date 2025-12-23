package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/tap"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/storage"
	"github.com/jcalabro/atlas/pkg/atlas"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type Args struct {
	TapAddr     string
	MetricsAddr string

	FDB foundation.Config
}

type ingester struct {
	log    *slog.Logger
	tracer trace.Tracer

	shutdownOnce sync.Once

	tap   *tap.Websocket
	store *storage.Store
}

func (i *ingester) shutdown(cancel context.CancelFunc) {
	i.shutdownOnce.Do(func() {
		i.log.Info("shutdown initiated")
		cancel()
	})
}

func Run(ctx context.Context, args *Args) error {
	if err := metrics.InitTracing(ctx, "atlas.ingester"); err != nil {
		return err
	}

	db, err := foundation.Open(args.FDB)
	if err != nil {
		return err
	}

	log := slog.Default().With(slog.String("component", "ingester"))

	i := &ingester{
		log:    log,
		tracer: otel.Tracer("atlas.ingester"),
		store:  storage.New(db),
	}

	i.tap, err = tap.NewWebsocket(
		args.TapAddr,
		i.processMessage,
		tap.WithAcks(),
		tap.WithLogger(log),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize tap client: %w", err)
	}

	cancelOnce := &sync.Once{}
	ctx, cancelFn := context.WithCancel(ctx)
	cancel := func() {
		cancelOnce.Do(func() {
			cancelFn()
		})
	}

	errs, ctx := errgroup.WithContext(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-ctx.Done():
		case <-sig:
			i.log.Info("received shutdown signal")
			i.shutdown(cancel)
		}
	}()

	errs.Go(func() error {
		metrics.RunServer(ctx, cancel, args.MetricsAddr)
		return nil
	})

	errs.Go(func() error {
		i.log.Info("ingester running")
		if err := i.tap.Run(ctx); err != nil {
			return fmt.Errorf("failed to run tap websocket consumer: %w", err)
		}

		i.log.Info("ingester shutdown complete")
		return nil
	})

	return errs.Wait()
}

func (i *ingester) processMessage(ctx context.Context, ev *tap.Event) (err error) {
	ctx, span := i.tracer.Start(ctx, "processMessage", trace.WithAttributes(
		attribute.Int64("id", int64(ev.ID)),
		attribute.String("type", ev.Type),
	))

	start := time.Now()
	status := metrics.StatusError
	action := "unknown"
	defer func() {
		metrics.IngestMessages.WithLabelValues(action, status).Inc()
		metrics.IngestMessageDuration.WithLabelValues(status).Observe(time.Since(start).Seconds())
		metrics.SpanEnd(span, err)
	}()

	switch pl := ev.Payload().(type) {
	case *tap.IdentityEvent:
		if err := i.handleIdentityEvent(ctx, pl); err != nil {
			return fmt.Errorf("failed to handle identity event: %w", err)
		}
	case *tap.RecordEvent:
		if err := i.handleRecordEvent(ctx, pl); err != nil {
			return fmt.Errorf("failed to handle record event: %w", err)
		}
	default:
		return fmt.Errorf("unknown message type %q for message %d", ev.Payload(), ev.ID)
	}

	status = metrics.StatusOK
	return nil
}

func (i *ingester) handleIdentityEvent(ctx context.Context, ident *tap.IdentityEvent) (err error) {
	_, span := i.tracer.Start(ctx, "handleIdentityEvent", trace.WithAttributes(
		attribute.String("did", ident.DID),
		attribute.String("handle", ident.Handle),
		attribute.Bool("is_active", ident.IsActive),
		attribute.String("status", ident.Status),
	))
	defer func() { metrics.SpanEnd(span, err) }()

	actor := &atlas.Actor{
		Did:      ident.DID,
		Handle:   ident.Handle,
		IsActive: ident.IsActive,
		Status:   ident.Status,
	}
	if err := i.store.PutActor(actor); err != nil {
		return fmt.Errorf("put actor: %w", err)
	}
	return nil
}

func (i *ingester) handleRecordEvent(ctx context.Context, rec *tap.RecordEvent) (err error) {
	_, span := i.tracer.Start(ctx, "handleRecordEvent", trace.WithAttributes(
		attribute.String("did", rec.DID),
		attribute.String("collection", rec.Collection),
		attribute.String("rkey", rec.Rkey),
		attribute.String("action", rec.Action),
		attribute.Bool("live", rec.Live),
	))
	defer func() { metrics.SpanEnd(span, err) }()

	switch rec.Action {
	case "create", "update":
		record := &atlas.Record{
			Uri:        fmt.Sprintf("at://%s/%s/%s", rec.DID, rec.Collection, rec.Rkey),
			Cid:        rec.CID,
			Did:        rec.DID,
			Collection: rec.Collection,
			Rkey:       rec.Rkey,
			Value:      rec.Record,
			IndexedAt:  time.Now().Unix(),
		}
		if err := i.store.PutRecord(record); err != nil {
			return fmt.Errorf("put record: %w", err)
		}
	case "delete":
		if err := i.store.DeleteRecord(rec.DID, rec.Collection, rec.Rkey); err != nil {
			return fmt.Errorf("delete record: %w", err)
		}
	}
	return nil
}
