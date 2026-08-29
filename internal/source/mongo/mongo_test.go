package mongo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

// These need a MongoDB replica set (a standalone mongod has no change streams):
//
//	SLIPSTREAM_TEST_MONGO_URI=mongodb://127.0.0.1:27017/?replicaSet=rs0 \
//	  go test ./internal/source/mongo/
func mongoURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("SLIPSTREAM_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set SLIPSTREAM_TEST_MONGO_URI to run MongoDB source tests")
	}
	return uri
}

type fixture struct {
	uri        string
	client     *mongo.Client
	database   string
	collection string
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	uri := mongoURI(t)
	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping: %v", err)
	}

	f := &fixture{
		uri:        uri,
		client:     client,
		database:   fmt.Sprintf("slip_%s_%d", name, time.Now().UnixNano()%1e9),
		collection: "docs",
	}
	t.Cleanup(func() {
		_ = client.Database(f.database).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	return f
}

func (f *fixture) coll() *mongo.Collection {
	return f.client.Database(f.database).Collection(f.collection)
}

func (f *fixture) readerConfig() config.MongoDB {
	return config.MongoDB{
		URI:         f.uri,
		Database:    f.database,
		Collections: []string{f.collection},
		Snapshot:    true,
	}
}

func waitFor(t *testing.T, events <-chan cdc.ChangeEvent, match func(cdc.ChangeEvent) bool) cdc.ChangeEvent {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event stream closed before a matching event arrived")
			}
			if match(ev) {
				return ev
			}
		case <-timeout:
			t.Fatal("timed out waiting for an event")
		}
	}
}

func docID(ev cdc.ChangeEvent) (int64, bool) {
	values := ev.After
	if len(values) == 0 {
		values = ev.Before
	}
	switch v := values["_id"].(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

// Documents written while the snapshot runs must all arrive, by one path or the
// other. Recording the cluster time before the snapshot admits duplicates but
// never a gap, so this asserts coverage.
func TestSnapshotAndStreamCoverEveryDocument(t *testing.T) {
	f := newFixture(t, "boundary")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const preloaded = 3000
	docs := make([]any, 0, preloaded)
	for i := 1; i <= preloaded; i++ {
		docs = append(docs, bson.M{"_id": i, "note": "preloaded"})
	}
	if _, err := f.coll().InsertMany(ctx, docs); err != nil {
		t.Fatalf("preload: %v", err)
	}

	var (
		wmu     sync.Mutex
		written = map[int64]bool{}
	)
	writeCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for id := int64(preloaded + 1); ; id++ {
			if writeCtx.Err() != nil {
				return
			}
			if _, err := f.coll().InsertOne(writeCtx, bson.M{"_id": id, "note": "concurrent"}); err != nil {
				return
			}
			wmu.Lock()
			written[id] = true
			wmu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 8192)
	readCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(readCtx, source.ReadRequest{}, events)
	}()

	seen := map[int64]bool{}
	snapshotted, streamed := 0, 0
	writerStopAt := time.Now().Add(3 * time.Second)
	var target int64 = -1
	deadline := time.After(90 * time.Second)

	for {
		if target == -1 && time.Now().After(writerStopAt) {
			stopWriter()
			<-writerDone
			wmu.Lock()
			target = int64(len(written))
			wmu.Unlock()
			if target == 0 {
				t.Fatal("the writer committed nothing; the test would be vacuous")
			}
		}
		if target >= 0 && int64(len(seen)) >= preloaded+target {
			break
		}

		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("reader stopped after %d documents", len(seen))
			}
			if id, ok := docID(ev); ok {
				switch ev.Op {
				case cdc.OpRead:
					snapshotted++
					seen[id] = true
				case cdc.OpCreate:
					streamed++
					seen[id] = true
				}
			}
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out with %d of %d documents (snapshot=%d stream=%d)",
				len(seen), preloaded+target, snapshotted, streamed)
		}
	}

	if snapshotted == 0 || streamed == 0 {
		t.Fatalf("no boundary was exercised: snapshot=%d stream=%d", snapshotted, streamed)
	}

	var missing []int64
	for id := int64(1); id <= preloaded; id++ {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	wmu.Lock()
	for id := range written {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	wmu.Unlock()
	if len(missing) > 0 {
		t.Errorf("%d written documents were never delivered, e.g. %v", len(missing), missing[:min(len(missing), 10)])
	}
	t.Logf("snapshot=%d stream=%d unique=%d (preloaded=%d concurrent=%d)",
		snapshotted, streamed, len(seen), preloaded, target)
}

func TestStreamsInsertUpdateDeleteAndDrop(t *testing.T) {
	f := newFixture(t, "dml")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := f.coll().InsertOne(ctx, bson.M{"_id": 1, "note": "first"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 256)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	snap := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })
	if snap.After["note"] != "first" {
		t.Errorf("snapshot document = %v", snap.After)
	}

	if _, err := f.coll().InsertOne(ctx, bson.M{"_id": 2, "note": "inserted"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ins := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpCreate })
	if ins.After["note"] != "inserted" {
		t.Errorf("insert = %v", ins.After)
	}
	if ins.Position == "" {
		t.Error("streamed event carries no resume token")
	}

	if _, err := f.coll().UpdateOne(ctx, bson.M{"_id": 2},
		bson.M{"$set": bson.M{"note": "updated"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	upd := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpUpdate })
	// updateLookup is on by default, so a sink gets the whole document.
	if upd.After["note"] != "updated" || upd.After["_id"] == nil {
		t.Errorf("update after = %v, want the whole document", upd.After)
	}

	if _, err := f.coll().DeleteOne(ctx, bson.M{"_id": 2}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	del := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpDelete })
	if id, ok := docID(del); !ok || id != 2 {
		t.Errorf("delete before = %v, want _id 2", del.Before)
	}

	if err := f.coll().Drop(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	drop := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpTruncate })
	if drop.Table != f.collection {
		t.Errorf("drop event names %q, want %q", drop.Table, f.collection)
	}
}

// Resuming from a stored token must continue after that event and must not
// re-run the snapshot.
func TestResumeFromStoredToken(t *testing.T) {
	f := newFixture(t, "resume")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if _, err := f.coll().InsertOne(ctx, bson.M{"_id": 1, "note": "before"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 256)
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })
	if _, err := f.coll().InsertOne(ctx, bson.M{"_id": 2, "note": "streamed"}); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	streamed := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpCreate })
	resumeFrom := streamed.Position

	stop()
	for range events {
	}
	_ = reader.Close()

	if _, err := f.coll().InsertOne(ctx, bson.M{"_id": 3, "note": "while-down"}); err != nil {
		t.Fatalf("insert 3: %v", err)
	}

	resumed := New(f.readerConfig(), "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 256)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() {
		defer close(events2)
		_ = resumed.ReadChanges(runCtx2, source.ReadRequest{From: resumeFrom}, events2)
	}()

	seen := map[int64]bool{}
	for !seen[3] {
		ev := waitFor(t, events2, func(cdc.ChangeEvent) bool { return true })
		if ev.Op == cdc.OpRead {
			t.Fatal("resume re-ran the snapshot instead of continuing from the token")
		}
		if id, ok := docID(ev); ok {
			seen[id] = true
		}
	}
	if seen[2] {
		t.Error("the event the token points at was delivered again; ResumeAfter should continue past it")
	}
}
