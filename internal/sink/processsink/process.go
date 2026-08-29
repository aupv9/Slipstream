// Package processsink hands batches to an external program over its standard
// input, so a sink can be written in any language.
//
// The protocol is deliberately the smallest thing that works: one JSON object
// per line in, one JSON object per line back.
//
//	→ {"batch_id":"...","count":2,"events":[{...},{...}]}
//	← {"ok":true}
//	← {"ok":false,"error":"target rejected row 7"}
//
// A reply of ok:false — or a closed pipe, or a timeout — fails the batch, and
// the pipeline retries it under the sink's configured policy. Because a retry
// re-sends the same batch_id, the program can recognise a replay; it must be
// idempotent like every other sink.
package processsink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

// Sink runs one child process and feeds it batches.
type Sink struct {
	name string
	cfg  config.ProcessSink
	log  *slog.Logger

	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	closed bool
}

// request is one batch on the wire.
type request struct {
	BatchID string            `json:"batch_id"`
	Count   int               `json:"count"`
	Events  []cdc.ChangeEvent `json:"events"`
}

// response is what the program replies.
type response struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// New prepares the sink. The process starts on the first write, so a
// misconfigured pipeline fails while writing rather than at construction.
func New(name string, cfg config.ProcessSink, log *slog.Logger) (*Sink, error) {
	if len(cfg.Command) == 0 {
		return nil, fmt.Errorf("process sink %q: command is required", name)
	}
	return &Sink{name: name, cfg: cfg, log: log.With("sink", name)}, nil
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// Close shuts the child down.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.stopLocked()
}

// stopLocked ends the child politely: closing stdin is how the program is told
// to finish, and it is killed only if it will not.
func (s *Sink) stopLocked() error {
	cmd, stdin := s.takeLocked()
	if cmd == nil {
		return nil
	}
	if stdin != nil {
		_ = stdin.Close()
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			return fmt.Errorf("process sink %q: wait: %w", s.name, err)
		}
		return nil
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		return nil
	}
}

// killLocked ends the child now.
//
// This is the path after a timeout or a broken exchange, where waiting politely
// would hold the pipeline for as long as the program feels like taking — the
// very thing the timeout exists to prevent. The reap runs in the background so
// the caller returns immediately.
func (s *Sink) killLocked() {
	cmd, stdin := s.takeLocked()
	if cmd == nil {
		return
	}
	if stdin != nil {
		_ = stdin.Close()
	}
	_ = cmd.Process.Kill()
	go func() { _ = cmd.Wait() }()
}

// takeLocked detaches the current process from the sink.
func (s *Sink) takeLocked() (*exec.Cmd, io.WriteCloser) {
	cmd, stdin := s.cmd, s.stdin
	s.cmd, s.stdin, s.stdout = nil, nil, nil
	return cmd, stdin
}

// start launches the child process. The caller holds the lock.
func (s *Sink) startLocked(ctx context.Context) error {
	if s.closed {
		return fmt.Errorf("process sink %q: sink is closed", s.name)
	}
	if s.cmd != nil {
		return nil
	}

	// Not exec.CommandContext: the child's lifetime is managed explicitly so a
	// cancelled batch does not kill a process the next batch would reuse.
	cmd := exec.Command(s.cfg.Command[0], s.cfg.Command[1:]...)
	cmd.Dir = s.cfg.Dir
	if len(s.cfg.Env) > 0 {
		cmd.Env = append(os.Environ(), s.cfg.Env...)
	}
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("process sink %q: stdin: %w", s.name, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("process sink %q: stdout: %w", s.name, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("process sink %q: start %v: %w", s.name, s.cfg.Command, err)
	}

	s.cmd = cmd
	s.stdin = stdin
	// A large buffer so one oversized reply cannot wedge the protocol.
	s.stdout = bufio.NewReaderSize(stdout, 1<<20)
	s.log.Info("started sink process", "command", s.cfg.Command)
	return nil
}

// Write sends the batch and waits for the program's verdict.
func (s *Sink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.startLocked(ctx); err != nil {
		return err
	}

	req := request{
		BatchID: fmt.Sprintf("%s|%s|%s",
			batch[0].SourceID, batch[0].Position, batch[len(batch)-1].Position),
		Count:  len(batch),
		Events: batch,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("process sink %q: marshal batch: %w", s.name, err)
	}
	payload = append(payload, '\n')

	timedOut, err := s.exchange(ctx, s.stdin, s.stdout, payload)
	switch {
	case timedOut:
		// A timed-out exchange leaves a goroutine still writing to this
		// process's pipes, so the process can never be reused safely, whatever
		// restart_on_error says.
		s.killLocked()
	case err != nil && s.cfg.RestartOnError:
		// A broken pipe or a bad reply can leave the child in a state the next
		// batch cannot rely on, so it is replaced rather than reused.
		_ = s.stopLocked()
	}
	return err
}

// exchange writes one request and reads one reply, bounded by the timeout.
//
// The pipes are passed in rather than read from the struct: on a timeout the
// goroutine outlives this call, and it must not touch fields the caller is
// about to replace.
func (s *Sink) exchange(ctx context.Context, stdin io.Writer, stdout *bufio.Reader, payload []byte) (timedOut bool, err error) {
	timeout := s.cfg.Timeout.D()
	if timeout <= 0 {
		timeout = time.Minute
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		resp response
		err  error
	}
	done := make(chan result, 1)

	go func() {
		if _, err := stdin.Write(payload); err != nil {
			done <- result{err: fmt.Errorf("process sink %q: write batch: %w", s.name, err)}
			return
		}
		line, err := stdout.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 {
				done <- result{err: fmt.Errorf("process sink %q: the program exited without replying", s.name)}
				return
			}
			if len(line) == 0 {
				done <- result{err: fmt.Errorf("process sink %q: read reply: %w", s.name, err)}
				return
			}
		}
		var resp response
		if err := json.Unmarshal(trimNewline(line), &resp); err != nil {
			done <- result{err: fmt.Errorf("process sink %q: reply %q is not valid JSON: %w",
				s.name, truncate(line), err)}
			return
		}
		done <- result{resp: resp}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			return false, r.err
		}
		if !r.resp.OK {
			if r.resp.Error == "" {
				return false, fmt.Errorf("process sink %q: the program rejected the batch", s.name)
			}
			return false, fmt.Errorf("process sink %q: %s", s.name, r.resp.Error)
		}
		return false, nil

	case <-callCtx.Done():
		return true, fmt.Errorf("process sink %q: the program did not reply within %s", s.name, timeout)
	}
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func truncate(b []byte) string {
	const max = 120
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
