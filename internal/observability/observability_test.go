package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRegistryRendersPrometheusText(t *testing.T) {
	r := NewRegistry()
	Register(r)

	r.Add(MetricEventsWritten, []string{"pipeline", "p1", "sink", "mirror"}, 3)
	r.Add(MetricEventsWritten, []string{"pipeline", "p1", "sink", "mirror"}, 2)
	r.Add(MetricEventsWritten, []string{"pipeline", "p1", "sink", "hook"}, 7)
	r.Set(MetricLagBytes, []string{"pipeline", "p1"}, 4096)
	r.Set(MetricLeader, []string{"pipeline", "p1", "instance", "inst-a"}, 1)

	out := r.Render()
	for _, want := range []string{
		"# TYPE slipstream_events_written_total counter",
		`slipstream_events_written_total{pipeline="p1",sink="mirror"} 5`,
		`slipstream_events_written_total{pipeline="p1",sink="hook"} 7`,
		"# TYPE slipstream_source_lag_bytes gauge",
		`slipstream_source_lag_bytes{pipeline="p1"} 4096`,
		`slipstream_leader{pipeline="p1",instance="inst-a"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q\n---\n%s", want, out)
		}
	}

	// Undeclared series must not be rendered at all, or scrapers see noise.
	if strings.Contains(out, "slipstream_dead_lettered_total") {
		t.Error("a metric with no observations should not be emitted")
	}
}

// Metrics are written from the reader, the router and every sink worker at
// once, so the registry has to hold up under concurrent use.
func TestRegistryIsSafeUnderConcurrency(t *testing.T) {
	r := NewRegistry()
	Register(r)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 500 {
				r.Add(MetricEventsWritten, []string{"pipeline", "p", "sink", "s"}, 1)
				r.Set(MetricQueueDepth, []string{"pipeline", "p", "sink", "s"}, int64(i))
				_ = r.Render()
			}
		}(i)
	}
	wg.Wait()

	if got := r.Get(MetricEventsWritten, []string{"pipeline", "p", "sink", "s"}); got != 4000 {
		t.Errorf("counter = %d, want 4000", got)
	}
}

// A nil registry is the "metrics disabled" case; it must be usable, not a
// source of panics in production.
func TestNilRegistryIsUsable(t *testing.T) {
	var r *Registry
	r.Define("x", Counter, "")
	r.Add("x", []string{"a", "b"}, 1)
	r.Set("x", nil, 2)
	if got := r.Get("x", nil); got != 0 {
		t.Errorf("Get on a nil registry = %d, want 0", got)
	}
	if out := r.Render(); out != "" {
		t.Errorf("Render on a nil registry = %q, want empty", out)
	}
}

// One process can lead some pipelines and stand by for others, so readiness
// has to report them separately rather than as one flag.
func TestHealthTracksEachPipelineSeparately(t *testing.T) {
	r := NewRegistry()
	Register(r)
	health := &Health{}
	health.SetRole("orders", true)
	health.SetStreaming("orders", true)
	health.SetRole("customers", false)
	health.SetError("customers", errors.New("lease lost"))

	srv := NewServer("127.0.0.1:0", r, health, quiet())
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	body, code := get(t, "http://"+srv.Addr()+"/readyz")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !strings.Contains(body, "pipeline: orders role: leader streaming: true") {
		t.Errorf("orders line missing from:\n%s", body)
	}
	if !strings.Contains(body, "pipeline: customers role: standby") {
		t.Errorf("customers line missing from:\n%s", body)
	}
	if !strings.Contains(body, "lease lost") {
		t.Errorf("the last error should be visible:\n%s", body)
	}
}

func TestServerServesMetricsAndHealth(t *testing.T) {
	r := NewRegistry()
	Register(r)
	r.Set(MetricLagBytes, []string{"pipeline", "p1"}, 128)

	health := &Health{}
	health.SetRole("p1", true)
	health.SetStreaming("p1", true)

	// Port 0 lets the OS pick, so tests never collide.
	srv := NewServer("127.0.0.1:0", r, health, quiet())
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})

	base := "http://" + srv.Addr()
	body, code := get(t, base+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics status %d", code)
	}
	if !strings.Contains(body, `slipstream_source_lag_bytes{pipeline="p1"} 128`) {
		t.Errorf("/metrics body missing the gauge:\n%s", body)
	}

	if _, code := get(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz status %d", code)
	}

	body, code = get(t, base+"/readyz")
	if code != http.StatusOK {
		t.Errorf("/readyz status %d", code)
	}
	if !strings.Contains(body, "role: leader") || !strings.Contains(body, "streaming: true") {
		t.Errorf("/readyz body = %q", body)
	}
	if !strings.Contains(body, "pipeline: p1") {
		t.Errorf("/readyz should name each pipeline: %q", body)
	}

	// A standby is healthy and ready: it is meant to be idle.
	health.SetRole("p1", false)
	health.SetStreaming("p1", false)
	body, code = get(t, base+"/readyz")
	if code != http.StatusOK {
		t.Errorf("standby /readyz status %d, want 200: taking standbys out of service defeats the point", code)
	}
	if !strings.Contains(body, "role: standby") {
		t.Errorf("/readyz body = %q", body)
	}
}

func get(t *testing.T, url string) (string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(body), resp.StatusCode
}
