package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jcalabro/atlas/internal/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

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

func (s *server) ingest(ctx context.Context, args *Args) error {
	const (
		maxConsecutiveErrs = 5
		initialBackoff     = 1 * time.Second
	)

	errCount := 0
	backoff := initialBackoff

	for {
		// Check for shutdown before attempting connection
		if ctx.Err() != nil {
			s.log.Info("ingester shutting down")
			return nil
		}

		err := s.ingestOnce(ctx, args)
		if errors.Is(err, context.Canceled) {
			s.log.Info("ingester shutting down")
			return nil
		}

		if err == nil {
			errCount = 0
			backoff = initialBackoff
			s.log.Info("tap connection closed normally, reconnecting")
			continue
		}

		errCount++
		s.log.Error("tap connection failed",
			"err", err,
			"consecutive_errors", errCount,
		)

		if errCount >= maxConsecutiveErrs {
			return fmt.Errorf("tap connection failed %d consecutive times: %w", errCount, err)
		}

		s.log.Info("retrying tap connection", "consecutive_errors", errCount)

		select {
		case <-ctx.Done():
			s.log.Info("ingester shutting down during backoff")
			return nil
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, 10*time.Second)
	}
}

func (s *server) ingestOnce(ctx context.Context, args *Args) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, args.TapAddr, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to tap at %q: %w", args.TapAddr, err)
	}
	defer conn.Close() // nolint:errcheck

	s.log.Info("connected to tap", "addr", args.TapAddr)

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

		if err := s.processMessage(ctx, data); err != nil {
			s.log.Warn("failed to process message", "err", err)
		}
	}
}

func (s *server) processMessage(ctx context.Context, data []byte) (err error) {
	ctx, span := s.tracer.Start(ctx, "handleRecord", trace.WithAttributes(
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
		if err := s.handleRecordEvent(ctx, &msg); err != nil {
			return err
		}

	case "user":
		if msg.User == nil {
			return fmt.Errorf("user message %d has nil user field", msg.ID)
		}

		action = msg.User.Status
		if err := s.handleUserEvent(ctx, &msg); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown message type %q for message %d", msg.Type, msg.ID)
	}

	status = metrics.StatusOK
	return nil
}

func (s *server) handleUserEvent(ctx context.Context, msg *tapMessage) (err error) {
	_, span := s.tracer.Start(ctx, "handleUserEvent", trace.WithAttributes(
		attribute.Int64("id", msg.ID),
		attribute.String("did", msg.User.DID),
		attribute.String("handle", msg.User.Handle),
		attribute.Bool("is_active", msg.User.IsActive),
		attribute.String("status", msg.User.Status),
	))
	defer func() {
		metrics.SpanEnd(span, err)
	}()

	metrics.IngestMessages.WithLabelValues(metrics.StatusOK).Inc()

	// TODO (jrc): handle user event

	return nil
}

func (s *server) handleRecordEvent(ctx context.Context, msg *tapMessage) (err error) {
	_, span := s.tracer.Start(ctx, "handleRecordEvent", trace.WithAttributes(
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

	// TODO (jrc): handle record event

	return nil
}
