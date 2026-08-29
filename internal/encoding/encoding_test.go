package encoding

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/aupv9/slipstream/internal/cdc"
	eventpb "github.com/aupv9/slipstream/internal/encoding/eventpb"
)

func sample() cdc.ChangeEvent {
	return cdc.ChangeEvent{
		SourceID: "src",
		Schema:   "public",
		Table:    "users",
		Op:       cdc.OpUpdate,
		Before:   map[string]any{"id": 7, "name": "old"},
		After:    map[string]any{"id": 7, "name": "new", "score": 1.5, "active": true},
		Position: "0/1000",
		CommitTS: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
	}
}

func TestJSONRoundTrip(t *testing.T) {
	enc, err := New(FormatJSON)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	b, err := enc.Encode(sample())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var back cdc.ChangeEvent
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Table != "users" || back.Op != cdc.OpUpdate || back.Position != "0/1000" {
		t.Errorf("round trip lost fields: %+v", back)
	}
	if back.After["name"] != "new" {
		t.Errorf("after = %v", back.After)
	}
	if enc.ContentType() != "application/json" {
		t.Errorf("content type = %q", enc.ContentType())
	}
}

func TestProtobufRoundTrip(t *testing.T) {
	enc, err := New(FormatProtobuf)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	b, err := enc.Encode(sample())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var msg eventpb.ChangeEvent
	if err := proto.Unmarshal(b, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.SourceId != "src" || msg.Table != "users" || msg.Op != "u" {
		t.Errorf("round trip lost fields: %+v", &msg)
	}
	if got := msg.After.Fields["name"].GetStringValue(); got != "new" {
		t.Errorf("after.name = %q", got)
	}
	if got := msg.After.Fields["score"].GetNumberValue(); got != 1.5 {
		t.Errorf("after.score = %v", got)
	}
	if got := msg.After.Fields["active"].GetBoolValue(); !got {
		t.Error("after.active lost its true value")
	}
	if msg.Before.Fields["name"].GetStringValue() != "old" {
		t.Error("before image lost")
	}
	if msg.CommitTs.AsTime().UTC() != sample().CommitTS {
		t.Errorf("commit ts = %v", msg.CommitTs.AsTime())
	}
}

// Row values arrive as whatever a database driver produced. A type protobuf's
// Struct cannot hold must degrade to a readable string, not fail the event and
// stall the pipeline.
func TestProtobufFallsBackForExoticValues(t *testing.T) {
	enc, _ := New(FormatProtobuf)

	ev := sample()
	ev.After = map[string]any{
		"when":  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"bytes": []byte{1, 2, 3},
		"id":    int64(9),
	}

	b, err := enc.Encode(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var msg eventpb.ChangeEvent
	if err := proto.Unmarshal(b, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := msg.After.Fields["when"].GetStringValue(); got == "" {
		t.Errorf("timestamp value was dropped: %v", msg.After.Fields["when"])
	}
	if got := msg.After.Fields["id"].GetNumberValue(); got != 9 {
		t.Errorf("id = %v, want 9", got)
	}
}

// Protobuf exists to save bandwidth, so it should actually be smaller.
func TestProtobufIsSmallerThanJSON(t *testing.T) {
	jsonEnc, _ := New(FormatJSON)
	protoEnc, _ := New(FormatProtobuf)

	j, err := jsonEnc.Encode(sample())
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	p, err := protoEnc.Encode(sample())
	if err != nil {
		t.Fatalf("proto: %v", err)
	}
	if len(p) >= len(j) {
		t.Errorf("protobuf %d bytes is not smaller than json %d bytes", len(p), len(j))
	}
	t.Logf("json=%d bytes protobuf=%d bytes", len(j), len(p))
}

func TestUnknownFormatIsRejected(t *testing.T) {
	if _, err := New("avro"); err == nil {
		t.Fatal("expected an error for an unsupported format")
	}
	if _, err := New(""); err != nil {
		t.Fatalf("an empty format should default to JSON, got %v", err)
	}
}
