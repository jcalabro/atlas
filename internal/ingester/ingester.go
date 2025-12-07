package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/gorilla/websocket"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Args struct {
	TapAddr     string
	MetricsAddr string

	FDB foundation.Config
}

type tapMessage struct {
	ID     int64      `json:"id"`
	Type   string     `json:"type"`
	Record *tapRecord `json:"record,omitempty"`
	User   *tapUser   `json:"user,omitempty"`
}

type tapRecord struct {
	DID        string          `json:"did"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Action     string          `json:"action"`
	CID        string          `json:"cid"`
	Record     json.RawMessage `json:"record"`
	Live       bool            `json:"live"`
}

type tapUser struct {
	DID      string `json:"did"`
	Handle   string `json:"handle"`
	IsActive bool   `json:"isActive"`
	Status   string `json:"status"`
}

type ingester struct {
	log    *slog.Logger
	tracer trace.Tracer

	shutOnce sync.Once

	fdb fdb.Database
}

func (i *ingester) shutdown(cancel context.CancelFunc) {
	i.shutOnce.Do(func() {
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

	i := &ingester{
		log:    slog.Default().With(slog.String("component", "ingester")),
		tracer: otel.Tracer("atlas.ingester"),
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
			i.log.Info("received shutdown signal")
			i.shutdown(cancel)
		}
	}()

	if err := i.ingest(ctx, args); err != nil {
		return err
	}

	i.log.Info("ingester shutdown complete")
	return nil
}

func (i *ingester) ingest(ctx context.Context, args *Args) error {
	const (
		maxConsecutiveErrs = 5
		initialBackoff     = 1 * time.Second
	)

	errCount := 0
	backoff := initialBackoff

	for {
		if ctx.Err() != nil {
			i.log.Info("ingester shutting down")
			return nil
		}

		err := i.ingestOnce(ctx, args.TapAddr)
		if errors.Is(err, context.Canceled) {
			i.log.Info("ingester shutting down")
			return nil
		}

		if err == nil {
			errCount = 0
			backoff = initialBackoff
			i.log.Info("tap connection closed normally, reconnecting")
			continue
		}

		errCount++
		i.log.Error("tap connection failed",
			"err", err,
			"consecutive_errors", errCount,
		)

		if errCount >= maxConsecutiveErrs {
			return fmt.Errorf("tap connection failed %d consecutive times: %w", errCount, err)
		}

		i.log.Info("retrying tap connection", "consecutive_errors", errCount)

		select {
		case <-ctx.Done():
			i.log.Info("ingester shutting down during backoff")
			return nil
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, 10*time.Second)
	}
}

func (i *ingester) ingestOnce(ctx context.Context, tapAddr string) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, tapAddr, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to tap at %q: %w", tapAddr, err)
	}
	defer conn.Close() //nolint:errcheck

	i.log.Info("connected to tap", "addr", tapAddr)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			return fmt.Errorf("failed to set websocket read deadline: %w", err)
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}

			var netErr net.Error
			if errors.As(err, &netErr) && ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf("failed to read websocket message: %w", err)
		}

		if err := i.processMessage(ctx, data); err != nil {
			i.log.Warn("failed to process message", "err", err)
		}
	}
}

func (i *ingester) processMessage(ctx context.Context, data []byte) (err error) {
	ctx, span := i.tracer.Start(ctx, "processMessage", trace.WithAttributes(
		attribute.Int("data_len", len(data)),
	))

	start := time.Now()
	status := metrics.StatusError
	action := "unknown"
	defer func() {
		metrics.IngestMessages.WithLabelValues(action, status).Inc()
		metrics.IngestMessageDuration.WithLabelValues(status).Observe(time.Since(start).Seconds())
		metrics.SpanEnd(span, err)
	}()

	var msg tapMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	switch msg.Type {
	case "record":
		if msg.Record == nil {
			return fmt.Errorf("record message %d has nil record field", msg.ID)
		}

		action = msg.Record.Action
		if err := i.handleRecordEvent(ctx, &msg); err != nil {
			return err
		}

	case "user":
		if msg.User == nil {
			return fmt.Errorf("user message %d has nil user field", msg.ID)
		}

		action = msg.User.Status
		if err := i.handleUserEvent(ctx, &msg); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown message type %q for message %d", msg.Type, msg.ID)
	}

	status = metrics.StatusOK
	return nil
}

func (i *ingester) handleUserEvent(ctx context.Context, msg *tapMessage) (err error) {
	_, span := i.tracer.Start(ctx, "handleUserEvent", trace.WithAttributes(
		attribute.Int64("id", msg.ID),
		attribute.String("did", msg.User.DID),
		attribute.String("handle", msg.User.Handle),
		attribute.Bool("is_active", msg.User.IsActive),
		attribute.String("status", msg.User.Status),
	))
	defer func() {
		metrics.SpanEnd(span, err)
	}()

	return nil
}

func (i *ingester) handleRecordEvent(ctx context.Context, msg *tapMessage) (err error) {
	_, span := i.tracer.Start(ctx, "handleRecordEvent", trace.WithAttributes(
		attribute.Int64("id", msg.ID),
		attribute.String("did", msg.Record.DID),
		attribute.String("collection", msg.Record.Collection),
		attribute.String("rkey", msg.Record.Rkey),
		attribute.String("action", msg.Record.Action),
		attribute.Bool("live", msg.Record.Live),
	))
	defer func() {
		metrics.SpanEnd(span, err)
	}()

	return nil
}
