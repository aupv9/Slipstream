package mongo

import (
	"io"
	"log/slog"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func testReader() *Reader {
	return New(config.MongoDB{Database: "app"}, "src", quietLogger())
}

// raw builds a change stream document the way the server would send it.
func raw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bson.Raw(b)
}

func changeDoc(opType string, extra ...bson.E) bson.D {
	d := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "82650000"}}},
		{Key: "operationType", Value: opType},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "app"}, {Key: "coll", Value: "users"}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: 1700000000, I: 1}},
	}
	return append(d, extra...)
}

func TestInsertCarriesTheWholeDocument(t *testing.T) {
	r := testReader()
	id := bson.NewObjectID()

	ev, ok, err := r.toChangeEvent(raw(t, changeDoc("insert",
		bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: id}, {Key: "name", Value: "ada"}}},
		bson.E{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}},
	)))
	if err != nil || !ok {
		t.Fatalf("insert not mapped: ok=%v err=%v", ok, err)
	}

	if ev.Op != cdc.OpCreate {
		t.Errorf("op = %q, want %q", ev.Op, cdc.OpCreate)
	}
	if ev.Schema != "app" || ev.Table != "users" {
		t.Errorf("namespace = %s.%s, want app.users", ev.Schema, ev.Table)
	}
	if ev.After["name"] != "ada" {
		t.Errorf("after = %v", ev.After)
	}
	// An ObjectID must reach a JSON sink as its hex form, not as raw bytes.
	if got := ev.After["_id"]; got != id.Hex() {
		t.Errorf("_id = %v (%T), want the hex string %s", got, got, id.Hex())
	}
	if ev.Position != "token:82650000" {
		t.Errorf("position = %q, want the resume token", ev.Position)
	}
	if ev.CommitTS.IsZero() {
		t.Error("event has no cluster time")
	}
}

// With updateLookup the whole document is available, and that is what a sink
// upserting documents needs.
func TestUpdateUsesTheLookedUpDocument(t *testing.T) {
	r := testReader()
	ev, ok, err := r.toChangeEvent(raw(t, changeDoc("update",
		bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: 1}, {Key: "name", Value: "new"}}},
		bson.E{Key: "updateDescription", Value: bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "name", Value: "new"}}},
		}},
	)))
	if err != nil || !ok {
		t.Fatalf("update not mapped: ok=%v err=%v", ok, err)
	}
	if ev.Op != cdc.OpUpdate {
		t.Errorf("op = %q, want %q", ev.Op, cdc.OpUpdate)
	}
	if ev.After["name"] != "new" || ev.After["_id"] == nil {
		t.Errorf("after = %v, want the whole document", ev.After)
	}
}

// Without the lookup, only the changed fields exist. They must still be sent
// with the key, so a sink can apply them, rather than being dropped.
func TestUpdateWithoutLookupFallsBackToChangedFieldsPlusKey(t *testing.T) {
	r := testReader()
	ev, ok, err := r.toChangeEvent(raw(t, changeDoc("update",
		bson.E{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 42}}},
		bson.E{Key: "updateDescription", Value: bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "name", Value: "new"}}},
		}},
	)))
	if err != nil || !ok {
		t.Fatalf("update not mapped: ok=%v err=%v", ok, err)
	}
	if ev.After["name"] != "new" {
		t.Errorf("changed field missing: %v", ev.After)
	}
	if ev.After["_id"] == nil {
		t.Error("the document key must be present or the sink cannot locate the row")
	}
}

func TestDeleteCarriesTheDocumentKey(t *testing.T) {
	r := testReader()
	ev, ok, err := r.toChangeEvent(raw(t, changeDoc("delete",
		bson.E{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 7}}},
	)))
	if err != nil || !ok {
		t.Fatalf("delete not mapped: ok=%v err=%v", ok, err)
	}
	if ev.Op != cdc.OpDelete {
		t.Errorf("op = %q", ev.Op)
	}
	if ev.Before["_id"] == nil {
		t.Errorf("before = %v, want the document key", ev.Before)
	}
	if ev.After != nil {
		t.Errorf("after = %v, want nothing for a delete", ev.After)
	}
}

// A dropped collection removes every document in it. Ignoring the event would
// leave sinks holding all of them.
func TestDropBecomesATruncate(t *testing.T) {
	r := testReader()
	ev, ok, err := r.toChangeEvent(raw(t, changeDoc("drop")))
	if err != nil || !ok {
		t.Fatalf("drop not mapped: ok=%v err=%v", ok, err)
	}
	if ev.Op != cdc.OpTruncate {
		t.Errorf("op = %q, want %q", ev.Op, cdc.OpTruncate)
	}
}

func TestEventsWithNoRowChangeAreSkipped(t *testing.T) {
	r := testReader()
	for _, opType := range []string{"dropDatabase", "rename", "invalidate"} {
		d := changeDoc(opType)
		if opType == "dropDatabase" {
			d = bson.D{
				{Key: "_id", Value: bson.D{{Key: "_data", Value: "8265"}}},
				{Key: "operationType", Value: opType},
				{Key: "ns", Value: bson.D{{Key: "db", Value: "app"}}},
			}
		}
		_, ok, err := r.toChangeEvent(raw(t, d))
		if err != nil {
			t.Errorf("%s: %v", opType, err)
		}
		if ok {
			t.Errorf("%s produced a change event; it describes no row change", opType)
		}
	}
}

// The stored offset has to say how it should be resumed: a token resumes after
// that event, a timestamp starts the stream at that cluster time.
func TestPositionRoundTrip(t *testing.T) {
	ts := bson.Timestamp{T: 1700000000, I: 3}
	tsPos := position{Timestamp: &ts}
	if got := tsPos.String(); got != "ts:1700000000,3" {
		t.Fatalf("timestamp position = %q", got)
	}

	back, err := parsePosition(tsPos.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if back.Timestamp == nil || *back.Timestamp != ts {
		t.Errorf("round trip lost the timestamp: %+v", back)
	}
	if back.Token != "" {
		t.Error("a timestamp position must not look like a token")
	}

	tokenPos := position{Token: "82650000"}
	back, err = parsePosition(tokenPos.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if back.Token != "82650000" || back.Timestamp != nil {
		t.Errorf("round trip lost the token: %+v", back)
	}

	if _, err := parsePosition("0/1234"); err == nil {
		t.Error("an offset from another source type should be rejected, not misread")
	}
}

func TestNormalizeValueRendersBSONTypes(t *testing.T) {
	id := bson.NewObjectID()
	dec, err := bson.ParseDecimal128("3.14")
	if err != nil {
		t.Fatalf("decimal: %v", err)
	}

	got := normalizeDoc(bson.M{
		"id":     id,
		"amount": dec,
		"nested": bson.M{"inner": id},
		"list":   bson.A{id, "plain"},
		"ts":     bson.Timestamp{T: 5, I: 6},
	})

	if got["id"] != id.Hex() {
		t.Errorf("ObjectID = %v, want hex", got["id"])
	}
	if got["amount"] != "3.14" {
		t.Errorf("Decimal128 = %v, want a readable string", got["amount"])
	}
	if nested, ok := got["nested"].(map[string]any); !ok || nested["inner"] != id.Hex() {
		t.Errorf("nested document was not normalised: %v", got["nested"])
	}
	if list, ok := got["list"].([]any); !ok || list[0] != id.Hex() {
		t.Errorf("array was not normalised: %v", got["list"])
	}
	if got["ts"] != "5,6" {
		t.Errorf("Timestamp = %v", got["ts"])
	}
}
