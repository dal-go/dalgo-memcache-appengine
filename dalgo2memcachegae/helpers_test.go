package dalgo2memcachegae

import "testing"

func Test_isNil(t *testing.T) {
	if !isNil(nil) {
		t.Error("expected true for nil")
	}
	var p *int
	if !isNil(p) {
		t.Error("expected true for typed nil pointer")
	}
}
