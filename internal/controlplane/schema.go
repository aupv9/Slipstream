package controlplane

import _ "embed"

// Schema is the control-plane DDL, embedded so that `slipstream migrate` needs
// nothing but the binary and a connection string. It is idempotent: every
// instance may run it at startup.
//
//go:embed schema.sql
var Schema string
