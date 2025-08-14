package db_test

import (
	//"fmt"
	"bytes"
	"os"
	"testing"

	"github.com/matheusfrancisco/diskvgo/db"
)

func createTmpDB(t *testing.T, readOnly bool) *db.DB {
	t.Helper()
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	t.Cleanup(func() { os.Remove(name) })
	d, err := db.New(name, readOnly)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func setKey(t *testing.T, d *db.DB, key string, value []byte) {
	t.Helper()
	if err := d.SetKey(key, value); err != nil {
		t.Fatalf("Could not set key %q: %v", key, err)
	}
}

func getKey(t *testing.T, d *db.DB, key string) string {
	t.Helper()
	value, err := d.GetKey(key)
	if err != nil {
		t.Fatalf("Could not get key %q: %v", key, err)
	}
	return string(value)
}

func TestGetSet(t *testing.T) {
	d := createTmpDB(t, false)

	setKey(t, d, "party", []byte("Great"))

	value := getKey(t, d, "party")
	if value != "Great" {
		t.Errorf("Expected value 'Great', got '%s'", value)
	}

}


func TestDeleteKeys(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	d, err := db.New(name, false)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	defer d.Close()

	setKey(t, d, "party", []byte("Great"))
	setKey(t, d, "party2", []byte("Great2"))

	if err := d.DeleteReshardKeys(func(name string) bool {
		return name == "party2"
	}); err != nil {
		t.Fatalf("Could not delete key 'party': %v", err)
	}

	value := getKey(t, d, "party")
	if value != "Great" {
		t.Errorf("Expected value '', got '%s'", value)
	}

	value = getKey(t, d, "party2")
	if value != "" {
		t.Errorf("Expected value 'Great2', got '%s'", value)
	}
}


func TestDeleteReplicationKey(t *testing.T) {
	d := createTmpDB(t, false)

	setKey(t, d, "party", []byte("Great"))

	k, v, err := d.GetNextReplicasKey()
	if err != nil {
		t.Fatalf(`Unexpected error for GetNextKeyForReplication(): %v`, err)
	}

	if !bytes.Equal(k, []byte("party")) || !bytes.Equal(v, []byte("Great")) {
		t.Errorf(`GetNextKeyForReplication(): got %q, %q; want %q, %q`, k, v, "party", "Great")
	}

	if err := d.DeleteReplicationKey([]byte("party"), []byte("Bad")); err == nil {
		t.Fatalf(`DeleteReplicationKey("party", "Bad"): got nil error, want non-nil error`)
	}

	if err := d.DeleteReplicationKey([]byte("party"), []byte("Great")); err != nil {
		t.Fatalf(`DeleteReplicationKey("party", "Great"): got %q, want nil error`, err)
	}

	k, v, err = d.GetNextReplicasKey()
	if err != nil {
		t.Fatalf(`Unexpected error for GetNextKeyForReplication(): %v`, err)
	}

	if k != nil || v != nil {
		t.Errorf(`GetNextKeyForReplication(): got %v, %v; want nil, nil`, k, v)
	}
}

func TestSetReadOnly(t *testing.T) {
	d := createTmpDB(t, true)

	if err := d.SetKey("party", []byte("Bad")); err == nil {
		t.Fatalf("SetKey(%q, %q): got nil error, want non-nil error", "party", []byte("Bad"))
	}
}
