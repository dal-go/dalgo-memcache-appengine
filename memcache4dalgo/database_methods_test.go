package memcache4dalgo

import "testing"

func TestDatabase_ID_and_Schema(t *testing.T) {
	db := &database{}
	if db.ID() == "" {
		t.Error("ID() should not be empty")
	}
	if db.Schema() != nil {
		t.Error("Schema() must be nil as per implementation")
	}
}
