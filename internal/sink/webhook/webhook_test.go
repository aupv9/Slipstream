package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

type capture struct {
	mu       sync.Mutex
	bodies   []payload
	headers  []http.Header
	status   int
	failNext int
}

func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = append(c.headers, r.Header.Clone())

	var p payload
	if err := json.Unmarshal(body, &p); err == nil {
		c.bodies = append(c.bodies, p)
	}

	if c.failNext > 0 {
		c.failNext--
		http.Error(w, "not today", http.StatusInternalServerError)
		return
	}
	status := c.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
}

func (c *capture) received() []payload {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]payload(nil), c.bodies...)
}

func (c *capture) lastHeaders() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.headers) == 0 {
		return nil
	}
	return c.headers[len(c.headers)-1]
}

func events(n int) []cdc.ChangeEvent {
	out := make([]cdc.ChangeEvent, n)
	for i := range out {
		out[i] = cdc.ChangeEvent{
			SourceID: "src", Schema: "public", Table: "users", Op: cdc.OpCreate,
			After:    map[string]any{"id": i + 1},
			Position: "0/100" + string(rune('0'+i)),
		}
	}
	return out
}

func newSink(t *testing.T, url string, headers map[string]string) *Sink {
	t.Helper()
	s, err := New("hook", config.WebhookSink{
		URL:     url,
		Headers: headers,
		Timeout: config.Duration(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPostsTheWholeBatchWithItsBounds(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()

	s := newSink(t, srv.URL, map[string]string{"Authorization": "Bearer t0ken"})
	batch := events(4)
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := c.received()
	if len(got) != 1 {
		t.Fatalf("received %d requests, want 1 batched request", len(got))
	}
	if got[0].Count != len(batch) || len(got[0].Events) != len(batch) {
		t.Errorf("count = %d, events = %d, want %d", got[0].Count, len(got[0].Events), len(batch))
	}
	if got[0].First != batch[0].Position || got[0].Last != batch[len(batch)-1].Position {
		t.Errorf("bounds = %s..%s, want %s..%s",
			got[0].First, got[0].Last, batch[0].Position, batch[len(batch)-1].Position)
	}

	h := c.lastHeaders()
	if h.Get("Authorization") != "Bearer t0ken" {
		t.Errorf("configured header missing: %q", h.Get("Authorization"))
	}
	if h.Get("Content-Type") != "application/json" {
		t.Errorf("content type = %q", h.Get("Content-Type"))
	}
	if key := h.Get("Idempotency-Key"); !strings.Contains(key, batch[0].Position) {
		t.Errorf("idempotency key %q should identify the batch", key)
	}
}

// A retried batch must be recognisable as the same batch, or the receiver
// cannot dedupe it.
func TestRetriedBatchKeepsTheSameIdempotencyKey(t *testing.T) {
	c := &capture{failNext: 1}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()

	s := newSink(t, srv.URL, nil)
	batch := events(3)

	if err := s.Write(context.Background(), batch); err == nil {
		t.Fatal("a 500 response must be reported as an error so the pipeline retries")
	}
	first := c.lastHeaders().Get("Idempotency-Key")

	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second := c.lastHeaders().Get("Idempotency-Key"); second != first {
		t.Errorf("retry sent key %q, first attempt sent %q", second, first)
	}
}

func TestNon2xxIsAnError(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusFound} {
		c := &capture{status: status}
		srv := httptest.NewServer(http.HandlerFunc(c.handler))

		s := newSink(t, srv.URL, nil)
		err := s.Write(context.Background(), events(1))
		srv.Close()

		if err == nil {
			t.Errorf("status %d was accepted as success", status)
		}
	}
}

func TestEmptyBatchDoesNotCallTheEndpoint(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()

	s := newSink(t, srv.URL, nil)
	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(c.received()) != 0 {
		t.Error("an empty batch should not produce a request")
	}
}

func TestSlowEndpointIsCancelledByContext(t *testing.T) {
	// The handler always returns on its own; relying on cancellation to
	// unblock it would make httptest's shutdown wait on this test's own
	// behaviour rather than on the sink's.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	s := newSink(t, srv.URL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Write(ctx, events(1))
	if err == nil {
		t.Fatal("expected the write to fail when the context expires")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("the write took %s to give up; the context deadline was 100ms", elapsed)
	}
}
