package pipeline

import (
	"testing"

	"github.com/aupv9/slipstream/internal/controlplane"
)

// The offset alone is not enough to decide a resume: an offset written while a
// snapshot was still running covers only part of the data.
func TestPlanResume(t *testing.T) {
	cases := []struct {
		name        string
		offset      string
		offsetFound bool
		phase       string
		phaseFound  bool
		wantForce   bool
		wantFrom    string
	}{
		{
			name:        "completed snapshot with an offset resumes",
			offset:      "0/2000",
			offsetFound: true,
			phase:       controlplane.SnapshotDone,
			phaseFound:  true,
			wantForce:   false,
			wantFrom:    "0/2000",
		},
		{
			name:        "interrupted snapshot must not resume from its partial offset",
			offset:      "0/2000",
			offsetFound: true,
			phase:       controlplane.SnapshotRunning,
			phaseFound:  true,
			wantForce:   true,
			wantFrom:    "0/2000", // kept for the log line, not used as a start point
		},
		{
			name:       "fresh pipeline bootstraps",
			phaseFound: false,
			wantForce:  true,
		},
		{
			name:        "an offset with no recorded snapshot is not trusted",
			offset:      "0/2000",
			offsetFound: true,
			phaseFound:  false,
			wantForce:   true,
			wantFrom:    "0/2000",
		},
		{
			name:       "completed snapshot without an offset bootstraps again",
			phase:      controlplane.SnapshotDone,
			phaseFound: true,
			wantForce:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planResume(tc.offset, tc.offsetFound, tc.phase, tc.phaseFound)
			if got.ForceBootstrap != tc.wantForce {
				t.Errorf("ForceBootstrap = %v, want %v (reason: %s)", got.ForceBootstrap, tc.wantForce, got.Reason)
			}
			if got.From != tc.wantFrom {
				t.Errorf("From = %q, want %q", got.From, tc.wantFrom)
			}
			if got.Reason == "" {
				t.Error("every decision should explain itself in the log")
			}
		})
	}
}
