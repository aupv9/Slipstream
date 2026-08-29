package kafkasink

import (
	"strings"
	"testing"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/encoding"
)

func newTestSink(cfg config.KafkaSink) *Sink {
	enc, err := encoding.New(encoding.FormatJSON)
	if err != nil {
		panic(err)
	}
	return &Sink{name: "events", cfg: cfg, enc: enc}
}

func event(op cdc.Op, id int) cdc.ChangeEvent {
	ev := cdc.ChangeEvent{
		SourceID: "src", Schema: "public", Table: "users", Op: op,
		Position: "0/1000",
	}
	if op == cdc.OpDelete {
		ev.Before = map[string]any{"id": id, "name": "gone"}
	} else {
		ev.After = map[string]any{"id": id, "name": "ada"}
	}
	return ev
}

// Every change to one row must land on one partition, or the consumer can see
// an update before the insert it depends on.
func TestPartitionKeyIsStableForOneRow(t *testing.T) {
	s := newTestSink(config.KafkaSink{Keys: map[string][]string{"public.users": {"id"}}})

	insert := s.PartitionKey(event(cdc.OpCreate, 7))
	update := s.PartitionKey(event(cdc.OpUpdate, 7))
	del := s.PartitionKey(event(cdc.OpDelete, 7))
	other := s.PartitionKey(event(cdc.OpCreate, 8))

	if insert != update || update != del {
		t.Errorf("one row produced different keys: insert=%q update=%q delete=%q", insert, update, del)
	}
	if insert == other {
		t.Error("different rows share a key, which serialises the whole table needlessly")
	}
	if !strings.HasPrefix(insert, "public.users|") {
		t.Errorf("key %q should be qualified by the table so two tables cannot collide", insert)
	}
}

// A delete carries only a before image, so the key has to come from there.
func TestPartitionKeyUsesBeforeImageForDeletes(t *testing.T) {
	s := newTestSink(config.KafkaSink{Keys: map[string][]string{"public.users": {"id"}}})
	if got, want := s.PartitionKey(event(cdc.OpDelete, 7)), "public.users|7"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

// Without configured keys the table itself is the key: correct ordering, but
// capped at one partition. That trade-off should be explicit, not accidental.
func TestPartitionKeyFallsBackToTheTable(t *testing.T) {
	s := newTestSink(config.KafkaSink{})
	if got, want := s.PartitionKey(event(cdc.OpCreate, 7)), "public.users"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

func TestTopicNaming(t *testing.T) {
	ev := event(cdc.OpCreate, 1)

	if got, want := Topic("cdc", "", ev), "cdc.public.users"; got != want {
		t.Errorf("topic = %q, want %q", got, want)
	}
	if got, want := Topic("cdc", "everything", ev), "everything"; got != want {
		t.Errorf("a fixed topic should win: got %q, want %q", got, want)
	}

	odd := ev
	odd.Table = "we ird/name"
	if got := Topic("", "", odd); strings.ContainsAny(got, " /") {
		t.Errorf("topic %q contains characters Kafka rejects", got)
	}
}

func TestMessageCarriesIdempotencyKey(t *testing.T) {
	s := newTestSink(config.KafkaSink{Keys: map[string][]string{"public.users": {"id"}}})
	msg, err := s.Message(event(cdc.OpCreate, 7))
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["idempotency-key"] != "src|public.users|0/1000" {
		t.Errorf("idempotency-key = %q", headers["idempotency-key"])
	}
	if headers["slipstream-op"] != "c" {
		t.Errorf("slipstream-op = %q", headers["slipstream-op"])
	}
	if !strings.Contains(string(msg.Value), `"after"`) {
		t.Errorf("payload does not look like a change event: %s", msg.Value)
	}
}
