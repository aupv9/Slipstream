// Package kafkasink publishes change events to Kafka.
package kafkasink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/encoding"
)

// Sink writes one Kafka message per change event.
type Sink struct {
	name   string
	cfg    config.KafkaSink
	enc    encoding.Encoder
	writer *kafka.Writer
}

// New builds a Kafka writer. It does not connect until the first write.
func New(name string, cfg config.KafkaSink, format encoding.Format) (*Sink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink %q: brokers are required", name)
	}
	enc, err := encoding.New(format)
	if err != nil {
		return nil, fmt.Errorf("kafka sink %q: %w", name, err)
	}

	w := &kafka.Writer{
		Addr:  kafka.TCP(cfg.Brokers...),
		Async: false, // a write must not be reported as done before the broker says so
		// All in-sync replicas must acknowledge. Anything weaker means a
		// leader failover can lose events the pipeline already committed past.
		RequiredAcks:           kafka.RequireAll,
		Balancer:               &kafka.Hash{}, // same key, same partition, so per-row order holds
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           cfg.Timeout.D(),
		AllowAutoTopicCreation: cfg.AutoCreateTopics,
	}
	if cfg.Compression == "gzip" {
		w.Compression = kafka.Gzip
	}

	return &Sink{name: name, cfg: cfg, enc: enc, writer: w}, nil
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// Close flushes and shuts the writer down.
func (s *Sink) Close() error {
	if s.writer == nil {
		return nil
	}
	if err := s.writer.Close(); err != nil {
		return fmt.Errorf("kafka sink %q: close: %w", s.name, err)
	}
	return nil
}

// Write publishes the batch and waits for the brokers to acknowledge it.
//
// Kafka gives no server-side deduplication here, so consumers must be
// idempotent: every message carries the event's idempotency key in a header for
// exactly that purpose.
func (s *Sink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, 0, len(batch))
	for _, ev := range batch {
		m, err := s.Message(ev)
		if err != nil {
			return err
		}
		msgs = append(msgs, m)
	}

	if err := s.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka sink %q: write %d messages: %w", s.name, len(msgs), err)
	}
	return nil
}

// Message builds the Kafka message for one event. Exported so the mapping can
// be tested without a broker.
func (s *Sink) Message(ev cdc.ChangeEvent) (kafka.Message, error) {
	payload, err := s.enc.Encode(ev)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("kafka sink %q: %w", s.name, err)
	}
	return kafka.Message{
		Topic: Topic(s.cfg.TopicPrefix, s.cfg.Topic, ev),
		Key:   []byte(s.PartitionKey(ev)),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "slipstream-op", Value: []byte(ev.Op)},
			{Key: "slipstream-source", Value: []byte(ev.SourceID)},
			{Key: "slipstream-position", Value: []byte(ev.Position)},
			{Key: "idempotency-key", Value: []byte(ev.IdempotencyKey())},
			{Key: "content-type", Value: []byte(s.enc.ContentType())},
		},
	}, nil
}

// PartitionKey decides which partition an event lands on, and therefore what
// stays ordered.
//
// With key columns configured for the table, the key is the row's identity, so
// every change to one row is ordered relative to the others. Without them the
// key is the table, which keeps the whole table ordered on one partition —
// correct, but no wider than a single partition's throughput.
func (s *Sink) PartitionKey(ev cdc.ChangeEvent) string {
	keys, ok := s.cfg.Keys[ev.Qualified()]
	if !ok {
		keys = s.cfg.Keys[ev.Table]
	}
	if len(keys) == 0 {
		return ev.Qualified()
	}

	values := ev.After
	if len(values) == 0 {
		values = ev.Before
	}

	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, ev.Qualified())
	for _, k := range keys {
		parts = append(parts, fmt.Sprint(values[k]))
	}
	return strings.Join(parts, "|")
}

// Topic is the destination topic: the fixed topic when configured, otherwise
// <prefix>.<schema>.<table>.
func Topic(prefix, fixed string, ev cdc.ChangeEvent) string {
	if fixed != "" {
		return fixed
	}
	parts := make([]string, 0, 3)
	if prefix != "" {
		parts = append(parts, prefix)
	}
	if ev.Schema != "" {
		parts = append(parts, ev.Schema)
	}
	parts = append(parts, ev.Table)
	return sanitizeTopic(strings.Join(parts, "."))
}

// sanitizeTopic keeps the name inside what Kafka accepts: letters, digits and
// . _ -
func sanitizeTopic(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
