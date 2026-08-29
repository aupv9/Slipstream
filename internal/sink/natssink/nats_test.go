package natssink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

// These tests need a NATS server with JetStream enabled:
//
//	nats-server -js -p 4222 &
//	SLIPSTREAM_TEST_NATS_URL=nats://127.0.0.1:4222 go test ./internal/sink/natssink/
func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SLIPSTREAM_TEST_NATS_URL")
	if url == "" {
		t.Skip("set SLIPSTREAM_TEST_NATS_URL to run NATS sink tests")
	}
	return url
}

// stream creates a throwaway JetStream stream and returns its subject prefix.
func stream(t *testing.T, url string) (prefix string, js nats.JetStreamContext, conn *nats.Conn) {
	t.Helper()

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	js, err = conn.JetStream()
	if err != nil {
		conn.Close()
		t.Fatalf("jetstream: %v", err)
	}

	prefix = fmt.Sprintf("slip%d", time.Now().UnixNano()%1e9)
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     prefix,
		Subjects: []string{prefix + ".>"},
		Storage:  nats.MemoryStorage,
		// The window inside which a repeated Nats-Msg-Id is discarded.
		Duplicates: time.Minute,
	}); err != nil {
		conn.Close()
		t.Fatalf("add stream: %v", err)
	}

	t.Cleanup(func() {
		_ = js.DeleteStream(prefix)
		conn.Close()
	})
	return prefix, js, conn
}

func testEvents(n int) []cdc.ChangeEvent {
	out := make([]cdc.ChangeEvent, n)
	for i := range out {
		out[i] = cdc.ChangeEvent{
			SourceID: "src", Schema: "public", Table: "users", Op: cdc.OpCreate,
			After:    map[string]any{"id": i + 1},
			Position: fmt.Sprintf("0/%04X", i+1),
		}
	}
	return out
}

func TestPublishesEveryEvent(t *testing.T) {
	url := natsURL(t)
	prefix, js, _ := stream(t, url)

	s, err := New("events", config.NATSSink{
		URL: url, SubjectPrefix: prefix,
		AckWait: config.Duration(10 * time.Second), MaxPending: 64,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer s.Close()

	batch := testEvents(20)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Write(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}

	info, err := js.StreamInfo(prefix)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != uint64(len(batch)) {
		t.Fatalf("stream holds %d messages, want %d", info.State.Msgs, len(batch))
	}

	sub, err := js.SubscribeSync(prefix+".>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatalf("next msg: %v", err)
	}
	if want := prefix + ".public.users"; msg.Subject != want {
		t.Errorf("subject = %q, want %q", msg.Subject, want)
	}
	if got := msg.Header.Get(nats.MsgIdHdr); got != batch[0].IdempotencyKey() {
		t.Errorf("message id = %q, want the event's idempotency key %q", got, batch[0].IdempotencyKey())
	}
}

// A replay after failover must not duplicate: JetStream discards a repeated
// Nats-Msg-Id inside the stream's duplicate window, which is exactly why the
// sink sets it.
func TestRepublishingTheSameEventsIsDeduplicated(t *testing.T) {
	url := natsURL(t)
	prefix, js, _ := stream(t, url)

	s, err := New("events", config.NATSSink{
		URL: url, SubjectPrefix: prefix,
		AckWait: config.Duration(10 * time.Second), MaxPending: 64,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer s.Close()

	batch := testEvents(10)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for range 3 {
		if err := s.Write(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	info, err := js.StreamInfo(prefix)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != uint64(len(batch)) {
		t.Fatalf("stream holds %d messages after 3 identical writes, want %d",
			info.State.Msgs, len(batch))
	}
}

// Publishing to a subject no stream covers must fail, not be silently dropped.
func TestWriteFailsWhenNoStreamAcceptsTheSubject(t *testing.T) {
	url := natsURL(t)
	stream(t, url) // a stream exists, but not for this prefix

	s, err := New("events", config.NATSSink{
		URL: url, SubjectPrefix: "nobody-listens-here",
		AckWait: config.Duration(3 * time.Second), MaxPending: 8,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Write(ctx, testEvents(1)); err == nil {
		t.Fatal("expected an error when no stream stores the subject")
	}
}

func TestSubjectSanitisesTableNames(t *testing.T) {
	ev := cdc.ChangeEvent{Schema: "pub.lic", Table: "we>ird"}
	if got, want := Subject("cdc", ev), "cdc.pub_lic.we_ird"; got != want {
		t.Errorf("subject = %q, want %q: wildcards must not widen the subject", got, want)
	}
}
