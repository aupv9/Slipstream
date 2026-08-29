// Package config loads the single YAML file that describes one pipeline.
//
// One process runs exactly one pipeline; running the same file on two or more
// hosts is what gives the pipeline HA, since only the instance holding the
// lease attaches to the source.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from strings like "10s".
type Duration time.Duration

// UnmarshalYAML parses a Go duration string.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Config is the whole file.
type Config struct {
	// InstanceID identifies this process in the leases table. Defaults to
	// "<hostname>-<pid>", which is distinct enough to make split-brain
	// visible in the table if it ever happened.
	InstanceID    string        `yaml:"instance_id"`
	ControlPlane  ControlPlane  `yaml:"control_plane"`
	Pipeline      Pipeline      `yaml:"pipeline"`
	Observability Observability `yaml:"observability"`
}

// Observability configures the metrics and health endpoints.
type Observability struct {
	// Addr is the listen address for /metrics, /healthz and /readyz. Empty
	// disables the endpoints entirely.
	Addr string `yaml:"addr"`
}

// ControlPlane points at the small Postgres holding offsets and leases.
type ControlPlane struct {
	DSN string `yaml:"dsn"`
	// LeaseTTL is how long a lease stays valid without renewal. Failover
	// takes at most this long.
	LeaseTTL Duration `yaml:"lease_ttl"`
	// LeaseRenew is the renewal period; keep it well under LeaseTTL so a
	// transient control-plane hiccup does not cost us the lease.
	LeaseRenew Duration `yaml:"lease_renew"`
	// AutoMigrate applies the embedded control-plane DDL on startup.
	AutoMigrate bool `yaml:"auto_migrate"`
}

// Pipeline is one source feeding a set of sinks.
type Pipeline struct {
	ID     string       `yaml:"id"`
	Source Source       `yaml:"source"`
	Sinks  []SinkConfig `yaml:"sinks"`
	// CommitInterval is how often the slowest sink cursor is promoted to the
	// pipeline offset (and acknowledged back to the source).
	CommitInterval Duration `yaml:"commit_interval"`
}

// Source selects and configures the reader.
type Source struct {
	// Type is one of postgres, mysql, mongodb.
	Type string `yaml:"type"`
	// ID is stamped on every event as source_id; it is part of the sink
	// dedupe key, so changing it re-delivers everything.
	ID       string   `yaml:"id"`
	Postgres Postgres `yaml:"postgres"`
	MySQL    MySQL    `yaml:"mysql"`
	MongoDB  MongoDB  `yaml:"mongodb"`
}

// Postgres configures the logical-replication reader.
type Postgres struct {
	// DSN must reach the primary with the replication privilege. The reader
	// appends replication=database itself for the streaming connection.
	DSN string `yaml:"dsn"`
	// Slot is the logical replication slot; it is created on first run and
	// then reused, which is what makes resume possible.
	Slot string `yaml:"slot"`
	// Publication lists the tables pgoutput will decode. Created from Tables
	// if missing.
	Publication string `yaml:"publication"`
	// Tables are "schema.table" names to capture.
	Tables []string `yaml:"tables"`
	// AutoAddTables adds tables that appear in Tables but are missing from an
	// existing publication. Those tables are captured streaming-only: they get
	// no snapshot, so rows written before they were added are never delivered.
	// Left false, a mismatch stops the pipeline with instructions instead.
	AutoAddTables bool `yaml:"auto_add_tables"`
	// Snapshot takes an initial consistent snapshot when no offset exists.
	Snapshot bool `yaml:"snapshot"`
}

// MySQL configures the binlog reader (see internal/source/mysql).
type MySQL struct {
	DSN       string   `yaml:"dsn"`
	ServerID  uint32   `yaml:"server_id"`
	Tables    []string `yaml:"tables"`
	Snapshot  bool     `yaml:"snapshot"`
	UseGTID   bool     `yaml:"use_gtid"`
	Heartbeat Duration `yaml:"heartbeat"`
}

// MongoDB configures the change-stream reader (see internal/source/mongo).
type MongoDB struct {
	URI          string   `yaml:"uri"`
	Database     string   `yaml:"database"`
	Collections  []string `yaml:"collections"`
	Snapshot     bool     `yaml:"snapshot"`
	FullDocument string   `yaml:"full_document"`
}

// SinkConfig configures one sink. Name must be unique inside the pipeline: it
// is the key of that sink's cursor row.
type SinkConfig struct {
	Name string `yaml:"name"`
	// Type is one of stdout, webhook, pgupsert.
	Type string `yaml:"type"`
	// QueueSize bounds how far this sink may lag before it applies
	// backpressure to the reader. This is the memory ceiling per sink.
	QueueSize int `yaml:"queue_size"`
	// BatchMaxEvents and BatchMaxWait control write batching.
	BatchMaxEvents int      `yaml:"batch_max_events"`
	BatchMaxWait   Duration `yaml:"batch_max_wait"`
	// RetryInitial and RetryMax bound the write backoff.
	RetryInitial Duration `yaml:"retry_initial"`
	RetryMax     Duration `yaml:"retry_max"`
	// OnFailure decides what happens when a sink keeps rejecting a batch.
	//
	// "retry" (the default) retries forever: nothing is ever lost, but one
	// permanently rejected event stalls this sink until someone intervenes.
	// "dead_letter" gives up after MaxAttempts, parks the offending events in
	// the control plane's dead_letters table and moves on — the only place
	// Slipstream deliberately stops delivering an event.
	OnFailure string `yaml:"on_failure"`
	// MaxAttempts is the attempt limit for OnFailure = dead_letter.
	MaxAttempts int `yaml:"max_attempts"`

	Webhook  WebhookSink  `yaml:"webhook"`
	PGUpsert PGUpsertSink `yaml:"pgupsert"`
	NATS     NATSSink     `yaml:"nats"`
	Kafka    KafkaSink    `yaml:"kafka"`
}

// NATSSink publishes events to NATS, one message per event.
type NATSSink struct {
	URL string `yaml:"url"`
	// SubjectPrefix is prepended to <schema>.<table>.
	SubjectPrefix string `yaml:"subject_prefix"`
	// CoreOnly publishes without JetStream. Core NATS does not acknowledge
	// publishes, so an event can be lost after Slipstream counted it
	// delivered; JetStream (the default) both acknowledges and deduplicates on
	// the message ID.
	CoreOnly bool `yaml:"core_only"`
	// MaxPending bounds in-flight async publishes.
	MaxPending int `yaml:"max_pending"`
	// AckWait bounds how long a batch waits for its acknowledgements.
	AckWait Duration `yaml:"ack_wait"`
	Token   string   `yaml:"token"`
	// CredentialsFile is a NATS .creds file for authentication.
	CredentialsFile string `yaml:"credentials_file"`
}

// KafkaSink publishes events to Kafka.
type KafkaSink struct {
	Brokers []string `yaml:"brokers"`
	// Topic pins every event to one topic. Left empty, the topic is
	// <topic_prefix>.<schema>.<table>.
	Topic       string `yaml:"topic"`
	TopicPrefix string `yaml:"topic_prefix"`
	// Keys maps "schema.table" to its key columns. With them, the partition
	// key is the row identity so changes to one row stay ordered; without
	// them the whole table shares one partition.
	Keys             map[string][]string `yaml:"keys"`
	Compression      string              `yaml:"compression"`
	AutoCreateTopics bool                `yaml:"auto_create_topics"`
	Timeout          Duration            `yaml:"timeout"`
}

// Failure policies for a sink that keeps rejecting a batch.
const (
	OnFailureRetry      = "retry"
	OnFailureDeadLetter = "dead_letter"
)

// WebhookSink posts batches of events as JSON.
type WebhookSink struct {
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Timeout Duration          `yaml:"timeout"`
}

// PGUpsertSink mirrors rows into a Postgres/warehouse table by primary key.
type PGUpsertSink struct {
	DSN string `yaml:"dsn"`
	// Schema is where target tables live; defaults to the event's own schema.
	Schema string `yaml:"schema"`
	// Keys maps "schema.table" to its key columns, used for the ON CONFLICT
	// target. A table with no entry here is rejected rather than silently
	// duplicated.
	Keys map[string][]string `yaml:"keys"`
	// SoftDelete keeps deleted rows and stamps a column instead of removing
	// them.
	SoftDelete bool   `yaml:"soft_delete"`
	DeletedCol string `yaml:"deleted_column"`
}

// Load reads, expands ${ENV} references, and validates the config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(raw))))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.InstanceID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		c.InstanceID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.ControlPlane.LeaseTTL == 0 {
		c.ControlPlane.LeaseTTL = Duration(10 * time.Second)
	}
	if c.ControlPlane.LeaseRenew == 0 {
		c.ControlPlane.LeaseRenew = Duration(c.ControlPlane.LeaseTTL.D() / 3)
	}
	if c.Pipeline.CommitInterval == 0 {
		c.Pipeline.CommitInterval = Duration(time.Second)
	}
	if c.Pipeline.Source.ID == "" {
		c.Pipeline.Source.ID = c.Pipeline.ID
	}
	if c.Pipeline.Source.Postgres.Slot == "" {
		c.Pipeline.Source.Postgres.Slot = slug("slipstream_" + c.Pipeline.ID)
	}
	if c.Pipeline.Source.Postgres.Publication == "" {
		c.Pipeline.Source.Postgres.Publication = slug("slipstream_" + c.Pipeline.ID)
	}

	for i := range c.Pipeline.Sinks {
		s := &c.Pipeline.Sinks[i]
		if s.Name == "" {
			s.Name = s.Type
		}
		if s.QueueSize == 0 {
			s.QueueSize = 4096
		}
		if s.BatchMaxEvents == 0 {
			s.BatchMaxEvents = 500
		}
		if s.BatchMaxWait == 0 {
			s.BatchMaxWait = Duration(200 * time.Millisecond)
		}
		if s.RetryInitial == 0 {
			s.RetryInitial = Duration(250 * time.Millisecond)
		}
		if s.RetryMax == 0 {
			s.RetryMax = Duration(30 * time.Second)
		}
		if s.Webhook.Timeout == 0 {
			s.Webhook.Timeout = Duration(30 * time.Second)
		}
		if s.NATS.AckWait == 0 {
			s.NATS.AckWait = Duration(30 * time.Second)
		}
		if s.NATS.MaxPending == 0 {
			s.NATS.MaxPending = 256
		}
		if s.Kafka.Timeout == 0 {
			s.Kafka.Timeout = Duration(30 * time.Second)
		}
		if s.PGUpsert.DeletedCol == "" {
			s.PGUpsert.DeletedCol = "_deleted_at"
		}
		if s.OnFailure == "" {
			s.OnFailure = OnFailureRetry
		}
	}
}

func (c *Config) validate() error {
	if c.ControlPlane.DSN == "" {
		return fmt.Errorf("config: control_plane.dsn is required")
	}
	if c.ControlPlane.LeaseRenew.D() >= c.ControlPlane.LeaseTTL.D() {
		return fmt.Errorf("config: control_plane.lease_renew (%s) must be shorter than lease_ttl (%s)",
			c.ControlPlane.LeaseRenew.D(), c.ControlPlane.LeaseTTL.D())
	}
	if c.Pipeline.ID == "" {
		return fmt.Errorf("config: pipeline.id is required")
	}
	if c.Pipeline.Source.Type == "" {
		return fmt.Errorf("config: pipeline.source.type is required")
	}
	if len(c.Pipeline.Sinks) == 0 {
		return fmt.Errorf("config: pipeline.sinks must list at least one sink")
	}

	seen := make(map[string]bool, len(c.Pipeline.Sinks))
	for _, s := range c.Pipeline.Sinks {
		if s.Type == "" {
			return fmt.Errorf("config: sink %q has no type", s.Name)
		}
		if seen[s.Name] {
			return fmt.Errorf("config: duplicate sink name %q; names key the sink_cursor rows", s.Name)
		}
		seen[s.Name] = true

		switch s.OnFailure {
		case OnFailureRetry:
			if s.MaxAttempts > 0 {
				return fmt.Errorf("config: sink %q sets max_attempts with on_failure: retry, "+
					"which would drop events once the limit is hit; use on_failure: dead_letter "+
					"to park them instead, or remove max_attempts to retry forever", s.Name)
			}
		case OnFailureDeadLetter:
			if s.MaxAttempts <= 0 {
				return fmt.Errorf("config: sink %q uses on_failure: dead_letter but no max_attempts, "+
					"so nothing would ever be parked", s.Name)
			}
		default:
			return fmt.Errorf("config: sink %q has on_failure %q, want %q or %q",
				s.Name, s.OnFailure, OnFailureRetry, OnFailureDeadLetter)
		}
	}
	return nil
}

// slug makes an identifier safe to use unquoted as a slot/publication name.
func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
