package doris

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/connector"
)

func TestSinkAdapterDoesNotRequireInitialSnapshotReset(t *testing.T) {
	var sink connector.Sink = &sinkAdapter{}
	if _, ok := sink.(connector.AuthoritativeInitialSnapshot); ok {
		t.Fatal("Doris sink must not require target reset for an initial snapshot")
	}
}
