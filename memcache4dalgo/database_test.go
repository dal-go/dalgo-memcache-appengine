package memcache4dalgo

import (
	"github.com/dal-go/dalgo/dal"
	"strings"
	"testing"
)

func TestNewDB(t *testing.T) {
	type args struct {
		db          *database
		isCacheable func(key *dal.Key) bool
	}
	tests := []struct {
		name        string
		args        args
		expectPanic string
	}{
		{
			name: "db=nil",
			args: args{
				db:          nil,
				isCacheable: nil,
			},
			expectPanic: "nil",
		},
		{
			name: "isCacheable",
			args: args{
				db: new(database),
				isCacheable: func(key *dal.Key) bool {
					return false
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic != "" {
				defer func() {
					if r := recover(); r == nil {
						t.Fatal("missing expected panic")
					} else if s, ok := r.(string); ok {
						if !strings.Contains(s, tt.expectPanic) {
							t.Errorf("expected panic %q, got %q", tt.expectPanic, r)
						}
					} else {
						t.Errorf("expected panic %q, got %v", tt.expectPanic, r)
					}
				}()
			}
			got := NewDB(tt.args.db, tt.args.isCacheable)
			if got == nil {
				t.Fatal("NewDB returned nil")
			}
			if got.(*database).db != tt.args.db {
				t.Errorf("NewDB().db = %v, want %v", got.(*database).db, tt.args.db)
			}
			if tt.args.isCacheable != nil && got.(*database).isCacheable == nil {
				t.Error("NewDB().isCacheable = nil")
			}
		})
	}
}
