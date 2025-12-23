package memcache4dalgo

import (
	"context"
	"strings"
	"testing"
)

func TestWarningf_DelegatesToDebugfWithPrefix(t *testing.T) {
	ctx := context.Background()
	var got string
	old := Debugf
	defer func() { Debugf = old }()
	Debugf = func(_ context.Context, format string, args ...any) {
		got = format
	}

	Warningf(ctx, "sample message: %s", "X")

	if !strings.HasPrefix(got, "WARNING: ") {
		t.Fatalf("expected prefix 'WARNING: ', got %q", got)
	}
	if !strings.Contains(got, "sample message:") {
		t.Fatalf("unexpected content: %q", got)
	}
}
