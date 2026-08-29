package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "slipstream.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimal = `
control_plane:
  dsn: postgres://cp/slipstream
pipeline:
  id: pg-main
  source:
    type: postgres
    postgres:
      dsn: postgres://src/app
      tables: [public.users]
  sinks:
    - name: hook
      type: webhook
      webhook:
        url: https://example.test/hook
`

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load(write(t, minimal))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.ControlPlane.LeaseTTL.D() != 10*time.Second {
		t.Errorf("lease_ttl = %s, want 10s", cfg.ControlPlane.LeaseTTL.D())
	}
	if cfg.ControlPlane.LeaseRenew.D() >= cfg.ControlPlane.LeaseTTL.D() {
		t.Errorf("default lease_renew %s must be shorter than the TTL", cfg.ControlPlane.LeaseRenew.D())
	}
	if cfg.InstanceID == "" {
		t.Error("instance_id should default to something host-specific")
	}
	if cfg.Pipeline.Source.ID != "pg-main" {
		t.Errorf("source id = %q, want the pipeline id", cfg.Pipeline.Source.ID)
	}
	if cfg.Pipeline.Source.Postgres.Slot != "slipstream_pg_main" {
		t.Errorf("slot = %q, want a slug of the pipeline id", cfg.Pipeline.Source.Postgres.Slot)
	}
	if cfg.Pipeline.Source.Postgres.Publication != "slipstream_pg_main" {
		t.Errorf("publication = %q", cfg.Pipeline.Source.Postgres.Publication)
	}
	if cfg.Pipeline.Sinks[0].QueueSize == 0 || cfg.Pipeline.Sinks[0].BatchMaxEvents == 0 {
		t.Error("sink batching defaults were not applied")
	}
	if cfg.Pipeline.Sinks[0].OnFailure != OnFailureRetry {
		t.Errorf("on_failure = %q, want %q: never drop by default",
			cfg.Pipeline.Sinks[0].OnFailure, OnFailureRetry)
	}
}

func TestLoadExpandsEnvReferences(t *testing.T) {
	t.Setenv("SLIPSTREAM_TEST_DSN", "postgres://secret@cp/slipstream")
	body := strings.Replace(minimal, "postgres://cp/slipstream", "${SLIPSTREAM_TEST_DSN}", 1)

	cfg, err := Load(write(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ControlPlane.DSN != "postgres://secret@cp/slipstream" {
		t.Errorf("dsn = %q, want the expanded value", cfg.ControlPlane.DSN)
	}
}

func TestLoadRejectsBadConfigs(t *testing.T) {
	cases := map[string]struct {
		body string
		want string
	}{
		"no control plane": {
			body: strings.Replace(minimal, "  dsn: postgres://cp/slipstream", "  auto_migrate: true", 1),
			want: "control_plane.dsn is required",
		},
		"unknown field": {
			body: minimal + "\n" + `x: y`,
			want: "field x not found",
		},
		"duplicate sink names": {
			body: minimal + `    - name: hook
      type: stdout
`,
			want: "duplicate sink name",
		},
		"no sinks": {
			body: `
control_plane:
  dsn: postgres://cp/slipstream
pipeline:
  id: p
  source:
    type: postgres
`,
			want: "must list at least one sink",
		},
		"max_attempts without dead_letter": {
			body: minimal + `      max_attempts: 3
`,
			want: "max_attempts with on_failure: retry",
		},
		"dead_letter without max_attempts": {
			body: minimal + `      on_failure: dead_letter
`,
			want: "no max_attempts",
		},
		"unknown failure policy": {
			body: minimal + `      on_failure: explode
`,
			want: `on_failure "explode"`,
		},
		"unknown duration": {
			body: strings.Replace(minimal, "  dsn: postgres://cp/slipstream",
				"  dsn: postgres://cp/slipstream\n  lease_ttl: 10 seconds", 1),
			want: "invalid duration",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(write(t, tc.body))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestLoadRejectsLeaseRenewAtOrAboveTTL(t *testing.T) {
	body := strings.Replace(minimal, "  dsn: postgres://cp/slipstream",
		"  dsn: postgres://cp/slipstream\n  lease_ttl: 5s\n  lease_renew: 5s", 1)

	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "lease_renew") {
		t.Fatalf("expected a lease_renew/lease_ttl error, got %v", err)
	}
}

const twoPipelines = `
control_plane:
  dsn: postgres://cp/slipstream
pipelines:
  - id: orders
    source:
      type: postgres
      postgres:
        dsn: postgres://src/orders
        tables: [public.orders]
    sinks:
      - name: hook
        type: webhook
        webhook:
          url: https://example.test/orders
  - id: customers
    source:
      type: postgres
      postgres:
        dsn: postgres://src/customers
        tables: [public.customers]
    sinks:
      - name: bus
        type: nats
        encoding: protobuf
        nats:
          url: nats://localhost:4222
`

func TestLoadSeveralPipelines(t *testing.T) {
	cfg, err := Load(write(t, twoPipelines))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	all := cfg.AllPipelines()
	if len(all) != 2 {
		t.Fatalf("got %d pipelines, want 2", len(all))
	}

	// Each pipeline needs its own slot and publication, or two pipelines would
	// fight over one replication slot.
	if all[0].Source.Postgres.Slot == all[1].Source.Postgres.Slot {
		t.Errorf("both pipelines default to slot %q", all[0].Source.Postgres.Slot)
	}
	if all[0].Source.Postgres.Slot != "slipstream_orders" {
		t.Errorf("slot = %q", all[0].Source.Postgres.Slot)
	}
	if all[1].Sinks[0].Encoding != "protobuf" {
		t.Errorf("encoding = %q", all[1].Sinks[0].Encoding)
	}
	if all[0].Sinks[0].QueueSize == 0 || all[1].Sinks[0].QueueSize == 0 {
		t.Error("sink defaults were not applied to every pipeline")
	}
}

func TestSinglePipelineStillWorksAndIsListed(t *testing.T) {
	cfg, err := Load(write(t, minimal))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	all := cfg.AllPipelines()
	if len(all) != 1 || all[0].ID != "pg-main" {
		t.Fatalf("AllPipelines() = %+v", all)
	}
	if cfg.Pipeline.ID != "pg-main" {
		t.Error("the single-pipeline field should still be populated")
	}
}

func TestRejectsBothPipelineForms(t *testing.T) {
	body := minimal + `
pipelines:
  - id: other
    source:
      type: postgres
    sinks:
      - name: s
        type: stdout
`
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected a clear error about using both forms, got %v", err)
	}
}

func TestRejectsDuplicatePipelineIDs(t *testing.T) {
	body := strings.Replace(twoPipelines, "- id: customers", "- id: orders", 1)
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "duplicate pipeline id") {
		t.Fatalf("expected a duplicate id error, got %v", err)
	}
}

func TestRejectsEncodingOnSinksThatHaveTheirOwnFormat(t *testing.T) {
	body := strings.Replace(minimal, "      type: webhook", "      type: webhook\n      encoding: protobuf", 1)
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "takes no encoding") {
		t.Fatalf("expected an encoding error, got %v", err)
	}
}
