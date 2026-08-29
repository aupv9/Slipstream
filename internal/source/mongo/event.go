package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/aupv9/slipstream/internal/cdc"
)

// toChangeEvent maps one change stream document onto a ChangeEvent.
//
// The bool is false for events that carry no row change (a dropped database,
// a rename) — they are skipped rather than mapped to something misleading.
func (r *Reader) toChangeEvent(raw bson.Raw) (cdc.ChangeEvent, bool, error) {
	opType, err := lookupString(raw, "operationType")
	if err != nil {
		return cdc.ChangeEvent{}, false, fmt.Errorf("mongodb: change event has no operationType: %w", err)
	}

	token, err := lookupString(raw, "_id", "_data")
	if err != nil {
		return cdc.ChangeEvent{}, false, fmt.Errorf("mongodb: change event has no resume token: %w", err)
	}

	database, _ := lookupString(raw, "ns", "db")
	collection, _ := lookupString(raw, "ns", "coll")

	ev := cdc.ChangeEvent{
		SourceID: r.sourceID,
		Schema:   database,
		Table:    collection,
		Position: position{Token: token}.String(),
		CommitTS: clusterTime(raw),
	}

	switch opType {
	case "insert":
		ev.Op = cdc.OpCreate
		ev.After = docAt(raw, "fullDocument")

	case "update", "replace":
		ev.Op = cdc.OpUpdate
		ev.After = docAt(raw, "fullDocument")
		if ev.After == nil {
			// Without updateLookup, or when the document was deleted before
			// the lookup ran, only the changed fields are available. Send them
			// with the key rather than pretending to have the whole document.
			ev.After = docAt(raw, "updateDescription", "updatedFields")
			if key := docAt(raw, "documentKey"); key != nil {
				if ev.After == nil {
					ev.After = map[string]any{}
				}
				for k, v := range key {
					ev.After[k] = v
				}
			}
		}
		ev.Before = docAt(raw, "fullDocumentBeforeChange")

	case "delete":
		ev.Op = cdc.OpDelete
		ev.Before = docAt(raw, "fullDocumentBeforeChange")
		if ev.Before == nil {
			// A delete carries the key; that is what a sink needs to find the
			// row.
			ev.Before = docAt(raw, "documentKey")
		}

	case "drop":
		// The collection is gone, so every document in it is gone. Silently
		// ignoring this would leave sinks holding all of them.
		ev.Op = cdc.OpTruncate

	default:
		// dropDatabase, rename, invalidate and the expanded DDL events carry
		// no per-document change.
		return cdc.ChangeEvent{}, false, nil
	}

	if ev.Table == "" {
		return cdc.ChangeEvent{}, false, nil
	}
	return ev, true, nil
}

func lookupString(raw bson.Raw, keys ...string) (string, error) {
	v, err := raw.LookupErr(keys...)
	if err != nil {
		return "", err
	}
	s, ok := v.StringValueOK()
	if !ok {
		return "", fmt.Errorf("value at %v is not a string", keys)
	}
	return s, nil
}

// docAt decodes a nested document into a plain map, or nil when absent.
func docAt(raw bson.Raw, keys ...string) map[string]any {
	v, err := raw.LookupErr(keys...)
	if err != nil {
		return nil
	}
	doc, ok := v.DocumentOK()
	if !ok {
		return nil
	}
	var m bson.M
	if err := bson.Unmarshal(doc, &m); err != nil {
		return nil
	}
	return normalizeDoc(m)
}

func clusterTime(raw bson.Raw) time.Time {
	v, err := raw.LookupErr("clusterTime")
	if err != nil {
		return time.Time{}
	}
	t, _, ok := v.TimestampOK()
	if !ok {
		return time.Time{}
	}
	return time.Unix(int64(t), 0).UTC()
}

// normalizeDoc converts BSON-specific types into ones a JSON sink can render
// without turning an ObjectID into an unreadable byte array.
func normalizeDoc(doc bson.M) map[string]any {
	if doc == nil {
		return nil
	}
	out := make(map[string]any, len(doc))
	for k, v := range doc {
		out[k] = normalizeValue(v)
	}
	return out
}

func normalizeValue(v any) any {
	switch t := v.(type) {
	case bson.ObjectID:
		return t.Hex()
	case bson.DateTime:
		return t.Time().UTC()
	case bson.Timestamp:
		return fmt.Sprintf("%d,%d", t.T, t.I)
	case bson.Decimal128:
		return t.String()
	case bson.M:
		return normalizeDoc(t)
	case map[string]any:
		return normalizeDoc(t)
	case bson.A:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = normalizeValue(item)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = normalizeValue(item)
		}
		return out
	default:
		return v
	}
}
