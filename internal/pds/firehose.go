package pds

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/pds/db"
	pdsmetrics "github.com/jcalabro/atlas/internal/pds/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// maxEventBatchSize is the maximum number of events to fetch per poll
	maxEventBatchSize = 100

	// pollInterval is how often to poll for new events when watch isn't available
	pollInterval = 50 * time.Millisecond

	// subscriberBufferSize is the size of each subscriber's event channel
	subscriberBufferSize = 1000

	// writeTimeout is the timeout for writing a single message to a websocket
	writeTimeout = 10 * time.Second

	// pongWait is how long to wait for pong response
	pongWait = 60 * time.Second

	// pingInterval is how often to send ping frames to keep connection alive
	pingInterval = 30 * time.Second
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow any origin for now
	},
}

// firehose manages the event stream for subscribeRepos
type firehose struct {
	log *slog.Logger
	db  *db.DB

	mu          sync.RWMutex
	subscribers map[*subscriber]struct{}
}

// subscriber represents a connected websocket client
type subscriber struct {
	id       string
	conn     *websocket.Conn
	connMu   sync.Mutex // protects writes to conn
	events   chan *types.RepoEvent
	pdsHost  string // empty means all hosts
	cancelFn context.CancelFunc
}

func newFirehose(log *slog.Logger, db *db.DB) *firehose {
	return &firehose{
		log:         log.With("component", "firehose"),
		db:          db,
		subscribers: make(map[*subscriber]struct{}),
	}
}

// Run starts the firehose event distribution loop.
// It watches FDB for new events and distributes them to all subscribers.
func (f *firehose) Run(ctx context.Context) {
	f.log.Info("starting firehose event loop")

	var cursor []byte
	var err error

	// get the current latest cursor to start from
	cursor, err = f.db.GetLatestSeq(ctx)
	if err != nil {
		f.log.Error("failed to get initial cursor", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			f.log.Info("firehose event loop shutting down")
			return
		default:
		}

		// try to set up a watch for new events
		watch, err := f.db.WatchLatestSeq(ctx)
		if err != nil {
			f.log.Warn("failed to set up watch, falling back to polling", "err", err)
			time.Sleep(pollInterval)
			continue
		}

		// wait for watch to trigger or context to cancel
		// we need to use a goroutine since FDB's Future doesn't have a channel interface
		watchDone := make(chan struct{})
		go func() {
			watch.BlockUntilReady()
			close(watchDone)
		}()

		select {
		case <-ctx.Done():
			watch.Cancel()
			return
		case <-watchDone:
			// new events available (or watch error)
		}

		// fetch and distribute new events
		cursor, err = f.pollAndDistribute(ctx, cursor)
		if err != nil {
			f.log.Error("error polling events", "err", err)
			time.Sleep(pollInterval)
		}
	}
}

// pollAndDistribute fetches new events from FDB and sends them to subscribers
func (f *firehose) pollAndDistribute(ctx context.Context, cursor []byte) ([]byte, error) {
	events, nextCursor, err := f.db.GetEventsSince(ctx, cursor, maxEventBatchSize)
	if err != nil {
		return cursor, fmt.Errorf("failed to get events: %w", err)
	}

	if len(events) == 0 {
		return cursor, nil
	}

	f.mu.RLock()
	subs := make([]*subscriber, 0, len(f.subscribers))
	for sub := range f.subscribers {
		subs = append(subs, sub)
	}
	f.mu.RUnlock()

	for _, event := range events {
		for _, sub := range subs {
			// filter by host if subscriber specified one
			if sub.pdsHost != "" && sub.pdsHost != event.PdsHost {
				continue
			}

			select {
			case sub.events <- event:
				pdsmetrics.FirehoseEventsSent.WithLabelValues(sub.pdsHost).Inc()
			default:
				// subscriber buffer full, drop event
				pdsmetrics.FirehoseEventsDropped.WithLabelValues(sub.pdsHost).Inc()
				f.log.Warn("dropping event for slow subscriber", "sub_id", sub.id)
			}
		}
	}

	return nextCursor, nil
}

// Subscribe adds a new subscriber and handles the websocket connection
func (f *firehose) Subscribe(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// parse cursor parameter
	cursorParam := r.URL.Query().Get("cursor")
	var cursor []byte
	if cursorParam != "" {
		seq, err := strconv.ParseInt(cursorParam, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		cursor = db.Int64ToSeq(seq)
	}

	// upgrade to websocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("failed to accept websocket: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	// create subscriber context
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// get pds host from request context (set by hostMiddleware)
	var pdsHost string
	if host := hostFromContext(r.Context()); host != nil {
		pdsHost = host.hostname
	}

	sub := &subscriber{
		id:       fmt.Sprintf("%s-%d", r.RemoteAddr, time.Now().UnixNano()),
		conn:     conn,
		events:   make(chan *types.RepoEvent, subscriberBufferSize),
		pdsHost:  pdsHost,
		cancelFn: cancel,
	}

	f.log.Info("new subscriber connected", "id", sub.id, "pds_host", pdsHost, "cursor", cursorParam)
	pdsmetrics.FirehoseSubscribers.WithLabelValues(pdsHost).Inc()
	defer func() {
		pdsmetrics.FirehoseSubscribers.WithLabelValues(pdsHost).Dec()
		f.log.Info("subscriber disconnected", "id", sub.id)
	}()

	// replay events from cursor if specified
	if cursor != nil {
		if err := f.replayEvents(subCtx, sub, cursor); err != nil {
			f.log.Error("failed to replay events", "err", err, "id", sub.id)
			return err
		}
	}

	// register subscriber for live events
	f.mu.Lock()
	f.subscribers[sub] = struct{}{}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.subscribers, sub)
		f.mu.Unlock()
	}()

	// configure connection for detecting disconnects
	conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
		return nil
	})

	// start goroutine to read from websocket (detect disconnects)
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				cancel()
				return
			}
		}
	}()

	// start goroutine to send ping frames periodically
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-subCtx.Done():
				return
			case <-ticker.C:
				sub.connMu.Lock()
				sub.conn.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck
				err := sub.conn.WriteMessage(websocket.PingMessage, nil)
				sub.connMu.Unlock()
				if err != nil {
					f.log.Debug("failed to send ping", "err", err, "id", sub.id)
					cancel()
					return
				}
			}
		}
	}()

	// main loop: send events to subscriber
	for {
		select {
		case <-subCtx.Done():
			return nil
		case event := <-sub.events:
			if err := f.sendEvent(sub, event); err != nil {
				f.log.Error("failed to send event", "err", err, "id", sub.id)
				return err
			}
		}
	}
}

// replayEvents sends historical events to a subscriber starting from the cursor
func (f *firehose) replayEvents(ctx context.Context, sub *subscriber, cursor []byte) error {
	for {
		events, nextCursor, err := f.db.GetEventsSince(ctx, cursor, maxEventBatchSize)
		if err != nil {
			return fmt.Errorf("failed to get events for replay: %w", err)
		}

		for _, event := range events {
			// filter by host if subscriber specified one
			if sub.pdsHost != "" && sub.pdsHost != event.PdsHost {
				continue
			}

			if err := f.sendEvent(sub, event); err != nil {
				return err
			}
		}

		if len(events) < maxEventBatchSize {
			// caught up
			return nil
		}

		cursor = nextCursor
	}
}

// sendEvent encodes and sends a single event to a subscriber
func (f *firehose) sendEvent(sub *subscriber, event *types.RepoEvent) error {
	var msg []byte
	var err error

	// encode based on event type
	switch event.EventType {
	case types.EventType_EVENT_TYPE_IDENTITY:
		msg, err = encodeIdentityEvent(event)
	case types.EventType_EVENT_TYPE_ACCOUNT:
		msg, err = encodeAccountEvent(event)
	default:
		// EVENT_TYPE_UNSPECIFIED and EVENT_TYPE_COMMIT are both commit events
		msg, err = encodeCommitEvent(event)
	}

	if err != nil {
		return fmt.Errorf("failed to encode event: %w", err)
	}

	sub.connMu.Lock()
	defer sub.connMu.Unlock()
	sub.conn.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck
	return sub.conn.WriteMessage(websocket.BinaryMessage, msg)
}

// encodeIdentityEvent converts a RepoEvent (identity type) to the ATProto CBOR wire format
func encodeIdentityEvent(event *types.RepoEvent) ([]byte, error) {
	identity := &atproto.SyncSubscribeRepos_Identity{
		Seq:    event.Seq,
		Did:    event.Repo,
		Handle: &event.Handle,
		Time:   event.Time.AsTime().Format(time.RFC3339Nano),
	}

	var buf bytes.Buffer

	header := events.EventHeader{
		Op:      events.EvtKindMessage,
		MsgType: "#identity",
	}
	if err := header.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}

	if err := identity.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal identity: %w", err)
	}

	return buf.Bytes(), nil
}

// encodeAccountEvent converts a RepoEvent (account type) to the ATProto CBOR wire format
func encodeAccountEvent(event *types.RepoEvent) ([]byte, error) {
	account := &atproto.SyncSubscribeRepos_Account{
		Seq:    event.Seq,
		Did:    event.Repo,
		Active: event.Active,
		Time:   event.Time.AsTime().Format(time.RFC3339Nano),
	}
	if event.Status != "" {
		account.Status = &event.Status
	}

	var buf bytes.Buffer

	header := events.EventHeader{
		Op:      events.EvtKindMessage,
		MsgType: "#account",
	}
	if err := header.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}

	if err := account.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal account: %w", err)
	}

	return buf.Bytes(), nil
}

// encodeCommitEvent converts a RepoEvent to the ATProto CBOR wire format
func encodeCommitEvent(event *types.RepoEvent) ([]byte, error) {
	// parse commit CID
	commitCID, err := cid.Cast(event.Commit)
	if err != nil {
		return nil, fmt.Errorf("failed to parse commit CID: %w", err)
	}

	// convert ops
	ops := make([]*atproto.SyncSubscribeRepos_RepoOp, 0, len(event.Ops))
	for _, op := range event.Ops {
		repoOp := &atproto.SyncSubscribeRepos_RepoOp{
			Action: op.Action,
			Path:   op.Path,
		}
		if len(op.Cid) > 0 {
			c, err := cid.Cast(op.Cid)
			if err != nil {
				return nil, fmt.Errorf("failed to parse op CID: %w", err)
			}
			ll := lexutil.LexLink(c)
			repoOp.Cid = &ll
		}
		ops = append(ops, repoOp)
	}

	// build the commit event
	commit := &atproto.SyncSubscribeRepos_Commit{
		Seq:    event.Seq,
		Repo:   event.Repo,
		Rev:    event.Rev,
		Since:  &event.Since,
		Commit: lexutil.LexLink(commitCID),
		Blocks: event.Blocks,
		Ops:    ops,
		Time:   event.Time.AsTime().Format(time.RFC3339Nano),
		TooBig: event.TooBig,
	}

	// encode header + body as CBOR
	var buf bytes.Buffer

	header := events.EventHeader{
		Op:      events.EvtKindMessage,
		MsgType: "#commit",
	}
	if err := header.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}

	if err := commit.MarshalCBOR(&buf); err != nil {
		return nil, fmt.Errorf("failed to marshal commit: %w", err)
	}

	return buf.Bytes(), nil
}

// handleSubscribeRepos is the HTTP handler for /xrpc/com.atproto.sync.subscribeRepos
func (s *server) handleSubscribeRepos(w http.ResponseWriter, r *http.Request) {
	if err := s.firehose.Subscribe(r.Context(), w, r); err != nil {
		s.log.Error("subscribeRepos error", "err", err)
	}
}

// EventBuilder helps construct events during repo mutations
type EventBuilder struct {
	pdsHost string
	repo    string
	rev     string
	since   string
	commit  cid.Cid
	ops     []*types.RepoOp
	blocks  *bytes.Buffer
}

// NewEventBuilder creates a new event builder for a repo mutation
func NewEventBuilder(pdsHost, repo, rev, since string, commit cid.Cid) *EventBuilder {
	return &EventBuilder{
		pdsHost: pdsHost,
		repo:    repo,
		rev:     rev,
		since:   since,
		commit:  commit,
		blocks:  new(bytes.Buffer),
	}
}

// AddOp adds an operation to the event
func (eb *EventBuilder) AddOp(action, path string, recordCID *cid.Cid) {
	op := &types.RepoOp{
		Action: action,
		Path:   path,
	}
	if recordCID != nil {
		op.Cid = recordCID.Bytes()
	}
	eb.ops = append(eb.ops, op)
}

// BlocksWriter returns the writer for CAR blocks
func (eb *EventBuilder) BlocksWriter() *bytes.Buffer {
	return eb.blocks
}

// Build constructs the final RepoEvent
func (eb *EventBuilder) Build() *types.RepoEvent {
	return &types.RepoEvent{
		PdsHost: eb.pdsHost,
		Repo:    eb.repo,
		Rev:     eb.rev,
		Since:   eb.since,
		Commit:  eb.commit.Bytes(),
		Blocks:  eb.blocks.Bytes(),
		Ops:     eb.ops,
		Time:    timestampNow(),
	}
}

// timestampNow returns the current time as a protobuf Timestamp
func timestampNow() *timestamppb.Timestamp {
	return timestamppb.Now()
}
