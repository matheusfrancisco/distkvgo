package db_test

import (
	//"fmt"
	"os"
	"testing"

	"github.com/matheusfrancisco/diskvgo/db"
)

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
	f, err := os.CreateTemp(os.TempDir(), "kvdb")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}
	//fmt.Println("Created temp file:", f.Name())

	name := f.Name()
	f.Close()
	defer os.Remove(name)

	d, err := db.New(name, false)
	if err != nil {
		t.Fatalf("Could not create a new database: %v", err)
	}
	defer d.Close()

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
