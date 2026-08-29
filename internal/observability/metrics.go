// Package observability exposes what an operator needs at 3am: how far behind
// the pipeline is, which instance is leading, and what the sinks are doing.
//
// The Prometheus text format is simple enough to write directly, so this costs
// no dependency — in keeping with a project whose whole point is not dragging
// infrastructure along.
package observability

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Kind distinguishes the two metric types we need.
type Kind string

const (
	// Counter only ever increases.
	Counter Kind = "counter"
	// Gauge goes up and down.
	Gauge Kind = "gauge"
)

// Registry holds the process's metrics.
type Registry struct {
	mu     sync.RWMutex
	series map[string]*family
	order  []string
}

type family struct {
	name    string
	kind    Kind
	help    string
	mu      sync.RWMutex
	values  map[string]*atomic.Int64
	labels  map[string][]string
	ordered []string
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{series: make(map[string]*family)}
}

// Define declares a metric. Declaring the same name twice is a no-op, so
// callers can define lazily.
//
// Every method tolerates a nil Registry, so code paths that do not care about
// metrics (tests, one-shot commands) need no guards.
func (r *Registry) Define(name string, kind Kind, help string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.series[name]; ok {
		return
	}
	r.series[name] = &family{
		name:   name,
		kind:   kind,
		help:   help,
		values: make(map[string]*atomic.Int64),
		labels: make(map[string][]string),
	}
	r.order = append(r.order, name)
}

// Add increases a counter (or moves a gauge) by delta.
func (r *Registry) Add(name string, labels []string, delta int64) {
	if r == nil {
		return
	}
	r.slot(name, labels).Add(delta)
}

// Set replaces a gauge's value.
func (r *Registry) Set(name string, labels []string, value int64) {
	if r == nil {
		return
	}
	r.slot(name, labels).Store(value)
}

// Get reads a value back, which is what makes the wiring testable.
func (r *Registry) Get(name string, labels []string) int64 {
	if r == nil {
		return 0
	}
	return r.slot(name, labels).Load()
}

func (r *Registry) slot(name string, labels []string) *atomic.Int64 {
	r.mu.RLock()
	f, ok := r.series[name]
	r.mu.RUnlock()
	if !ok {
		// An undeclared metric still records rather than panicking; a missing
		// HELP line is a much smaller problem than a crashed pipeline.
		r.Define(name, Gauge, "")
		r.mu.RLock()
		f = r.series[name]
		r.mu.RUnlock()
	}

	key := labelKey(labels)
	f.mu.RLock()
	v, ok := f.values[key]
	f.mu.RUnlock()
	if ok {
		return v
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.values[key]; ok {
		return v
	}
	v = new(atomic.Int64)
	f.values[key] = v
	f.labels[key] = append([]string(nil), labels...)
	f.ordered = append(f.ordered, key)
	return v
}

// Render writes the registry in Prometheus text exposition format.
func (r *Registry) Render() string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	names := append([]string(nil), r.order...)
	families := make([]*family, 0, len(names))
	for _, n := range names {
		families = append(families, r.series[n])
	}
	r.mu.RUnlock()

	var b strings.Builder
	for _, f := range families {
		f.mu.RLock()
		keys := append([]string(nil), f.ordered...)
		f.mu.RUnlock()
		if len(keys) == 0 {
			continue
		}
		sort.Strings(keys)

		if f.help != "" {
			fmt.Fprintf(&b, "# HELP %s %s\n", f.name, f.help)
		}
		fmt.Fprintf(&b, "# TYPE %s %s\n", f.name, f.kind)
		for _, k := range keys {
			f.mu.RLock()
			v := f.values[k]
			labels := f.labels[k]
			f.mu.RUnlock()
			fmt.Fprintf(&b, "%s%s %d\n", f.name, formatLabels(labels), v.Load())
		}
	}
	return b.String()
}

// labelKey joins alternating key/value pairs into a map key.
func labelKey(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return strings.Join(labels, "\x00")
}

// formatLabels renders alternating key/value pairs as {k="v",...}.
func formatLabels(labels []string) string {
	if len(labels) < 2 {
		return ""
	}
	parts := make([]string, 0, len(labels)/2)
	for i := 0; i+1 < len(labels); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%q", labels[i], escape(labels[i+1])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}
