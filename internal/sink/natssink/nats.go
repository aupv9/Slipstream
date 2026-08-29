// Package natssink publishes change events to NATS.
//
// JetStream is the default because it acknowledges publishes and deduplicates
// on a message ID. Core NATS is fire-and-forget: the publish returns before any
// server has durably accepted it, so a broker restart can lose events that
// Slipstream has already counted as delivered. That is available, but it has to
// be asked for.
package natssink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/encoding"
)

// Sink publishes one message per change event.
type Sink struct {
	name string
	cfg  config.NATSSink
	enc  encoding.Encoder
	conn *nats.Conn
	js   nats.JetStreamContext
}

// New connects to NATS.
func New(name string, cfg config.NATSSink, format encoding.Format) (*Sink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("nats sink %q: url is required", name)
	}
	enc, err := encoding.New(format)
	if err != nil {
		return nil, fmt.Errorf("nats sink %q: %w", name, err)
	}

	opts := []nats.Option{
		nats.Name("slipstream/" + name),
		// Keep trying rather than failing the pipeline on a blip; the router's
		// own retry handles anything this cannot absorb.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}

	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats sink %q: connect: %w", name, err)
	}

	s := &Sink{name: name, cfg: cfg, enc: enc, conn: conn}
	if !cfg.CoreOnly {
		js, err := conn.JetStream(nats.PublishAsyncMaxPending(cfg.MaxPending))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("nats sink %q: jetstream: %w", name, err)
		}
		s.js = js
	}
	return s, nil
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// Close drains and closes the connection.
func (s *Sink) Close() error {
	if s.conn == nil {
		return nil
	}
	if err := s.conn.Drain(); err != nil {
		s.conn.Close()
		return fmt.Errorf("nats sink %q: drain: %w", s.name, err)
	}
	return nil
}

// Write publishes the batch.
//
// On JetStream each message carries Nats-Msg-Id set to the event's idempotency
// key, so a replay after failover is discarded by the server inside its
// duplicate window rather than being delivered twice. Publishes are made
// asynchronously and then waited on together, which keeps a batch to roughly
// one round trip while still failing the batch if any message is not stored.
func (s *Sink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	if s.js == nil {
		return s.writeCore(batch)
	}

	futures := make([]nats.PubAckFuture, 0, len(batch))
	for _, ev := range batch {
		msg, err := s.message(ev)
		if err != nil {
			return err
		}
		f, err := s.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("nats sink %q: publish %s: %w", s.name, ev.Position, err)
		}
		futures = append(futures, f)
	}

	select {
	case <-s.js.PublishAsyncComplete():
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.cfg.AckWait.D()):
		return fmt.Errorf("nats sink %q: timed out waiting for publish acks", s.name)
	}

	for i, f := range futures {
		select {
		case err := <-f.Err():
			return fmt.Errorf("nats sink %q: %s not stored: %w", s.name, batch[i].Position, err)
		case <-f.Ok():
		default:
			return fmt.Errorf("nats sink %q: %s was neither acknowledged nor refused",
				s.name, batch[i].Position)
		}
	}
	return nil
}

// writeCore publishes without JetStream. The flush is the only confirmation
// available, and it only proves the bytes left this process.
func (s *Sink) writeCore(batch []cdc.ChangeEvent) error {
	for _, ev := range batch {
		msg, err := s.message(ev)
		if err != nil {
			return err
		}
		if err := s.conn.PublishMsg(msg); err != nil {
			return fmt.Errorf("nats sink %q: publish %s: %w", s.name, ev.Position, err)
		}
	}
	if err := s.conn.FlushTimeout(s.cfg.AckWait.D()); err != nil {
		return fmt.Errorf("nats sink %q: flush: %w", s.name, err)
	}
	return nil
}

func (s *Sink) message(ev cdc.ChangeEvent) (*nats.Msg, error) {
	payload, err := s.enc.Encode(ev)
	if err != nil {
		return nil, fmt.Errorf("nats sink %q: %w", s.name, err)
	}
	msg := nats.NewMsg(Subject(s.cfg.SubjectPrefix, ev))
	msg.Data = payload
	msg.Header.Set(nats.MsgIdHdr, ev.IdempotencyKey())
	msg.Header.Set("Slipstream-Op", string(ev.Op))
	msg.Header.Set("Slipstream-Source", ev.SourceID)
	msg.Header.Set("Content-Type", s.enc.ContentType())
	return msg, nil
}

// Subject is where an event is published: <prefix>.<schema>.<table>, with the
// parts NATS reserves replaced so a table named with a dot or wildcard cannot
// widen the subject it lands on.
func Subject(prefix string, ev cdc.ChangeEvent) string {
	parts := make([]string, 0, 3)
	if prefix != "" {
		parts = append(parts, prefix)
	}
	if ev.Schema != "" {
		parts = append(parts, sanitize(ev.Schema))
	}
	parts = append(parts, sanitize(ev.Table))
	return strings.Join(parts, ".")
}

func sanitize(s string) string {
	replacer := strings.NewReplacer(".", "_", "*", "_", ">", "_", " ", "_")
	return replacer.Replace(s)
}
