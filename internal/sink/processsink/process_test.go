package processsink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// script writes a small shell program acting as an external sink.
func script(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sink.sh")
	if err := os.WriteFile(path, []byte("#!/usr/bin/env bash\nset -u\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func newSink(t *testing.T, path string, mutate func(*config.ProcessSink)) *Sink {
	t.Helper()
	cfg := config.ProcessSink{
		Command: []string{path},
		Timeout: config.Duration(10 * time.Second),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New("external", cfg, quiet())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func events(n int) []cdc.ChangeEvent {
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

// The happy path: a program in any language reads a line, does its work, and
// answers ok.
func TestBatchesReachTheProgramAndAreAcknowledged(t *testing.T) {
	out := filepath.Join(t.TempDir(), "received.jsonl")
	s := newSink(t, script(t, fmt.Sprintf(`
while IFS= read -r line; do
  echo "$line" >> %q
  echo '{"ok":true}'
done
`, out)), nil)

	batch := events(3)
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Write(context.Background(), events(2)); err != nil {
		t.Fatalf("second write: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("program saw %d batches, want 2", len(lines))
	}

	var first request
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("the program received something that is not a batch: %v", err)
	}
	if first.Count != 3 || len(first.Events) != 3 {
		t.Errorf("count = %d, events = %d, want 3", first.Count, len(first.Events))
	}
	if first.Events[0].Table != "users" {
		t.Errorf("event lost its table: %+v", first.Events[0])
	}
	if !strings.Contains(first.BatchID, batch[0].Position) {
		t.Errorf("batch id %q should identify the batch so a replay is recognisable", first.BatchID)
	}
}

// A program that rejects a batch must fail the write, so the pipeline retries
// or dead-letters it rather than counting it delivered.
func TestRejectionIsAnError(t *testing.T) {
	s := newSink(t, script(t, `
while IFS= read -r line; do
  echo '{"ok":false,"error":"target rejected row 7"}'
done
`), nil)

	err := s.Write(context.Background(), events(1))
	if err == nil {
		t.Fatal("a rejected batch must be reported as an error")
	}
	if !strings.Contains(err.Error(), "target rejected row 7") {
		t.Errorf("the program's reason should survive: %v", err)
	}
}

// The same batch retried must arrive with the same id, so the program can
// recognise the replay rather than duplicating work.
func TestRetryReusesTheBatchID(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ids.txt")
	s := newSink(t, script(t, fmt.Sprintf(`
while IFS= read -r line; do
  echo "$line" | sed 's/.*"batch_id":"\([^"]*\)".*/\1/' >> %q
  echo '{"ok":false,"error":"nope"}'
done
`, out)), nil)

	batch := events(2)
	_ = s.Write(context.Background(), batch)
	_ = s.Write(context.Background(), batch)

	data, _ := os.ReadFile(out)
	ids := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(ids) != 2 {
		t.Fatalf("program saw %d attempts, want 2", len(ids))
	}
	if ids[0] != ids[1] {
		t.Errorf("retry used a different batch id: %q then %q", ids[0], ids[1])
	}
}

func TestProgramThatExitsIsReported(t *testing.T) {
	s := newSink(t, script(t, `exit 3`), nil)

	err := s.Write(context.Background(), events(1))
	if err == nil {
		t.Fatal("a program that exits without replying must fail the batch")
	}
}

func TestGarbageReplyIsReported(t *testing.T) {
	s := newSink(t, script(t, `
while IFS= read -r line; do
  echo 'this is not json'
done
`), nil)

	err := s.Write(context.Background(), events(1))
	if err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Fatalf("expected a clear parse error, got %v", err)
	}
}

// A program that hangs must not hang the pipeline with it.
func TestSlowProgramTimesOut(t *testing.T) {
	s := newSink(t, script(t, `
while IFS= read -r line; do
  sleep 30
  echo '{"ok":true}'
done
`), func(c *config.ProcessSink) {
		c.Timeout = config.Duration(300 * time.Millisecond)
		c.RestartOnError = true
	})

	start := time.Now()
	err := s.Write(context.Background(), events(1))
	if err == nil {
		t.Fatal("expected a timeout")
	}
	if !strings.Contains(err.Error(), "did not reply") {
		t.Errorf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the sink waited %s despite a 300ms timeout", elapsed)
	}
}

// After a failure with restart_on_error, the next batch gets a fresh process
// rather than one left in an unknown state.
func TestRestartAfterFailureUsesAFreshProcess(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "starts.txt")
	s := newSink(t, script(t, fmt.Sprintf(`
echo start >> %q
while IFS= read -r line; do
  echo '{"ok":false,"error":"always"}'
done
`, marker)), func(c *config.ProcessSink) { c.RestartOnError = true })

	_ = s.Write(context.Background(), events(1))
	_ = s.Write(context.Background(), events(1))

	data, _ := os.ReadFile(marker)
	starts := len(strings.Fields(string(data)))
	if starts != 2 {
		t.Errorf("the program started %d times, want a fresh one after each failure", starts)
	}
}

func TestEmptyBatchDoesNotStartTheProgram(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started.txt")
	s := newSink(t, script(t, fmt.Sprintf(`
echo start >> %q
while IFS= read -r line; do echo '{"ok":true}'; done
`, marker)), nil)

	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("an empty batch should not start the program")
	}
}

func TestCommandIsRequired(t *testing.T) {
	if _, err := New("external", config.ProcessSink{}, quiet()); err == nil {
		t.Fatal("expected an error when no command is configured")
	}
}
