package db_test

import (
	"bytes"
	//"fmt"
	"os"
	"testing"

	"github.com/matheusfrancisco/diskvgo/db"
)

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

	if err := d.SetKey("party", []byte("Great")); err != nil {
		t.Fatalf("Could not write key: %v", err)
	}

	value, err := d.GetKey("party")
	if err != nil {
		t.Fatalf(`Could not get the key "party": %v`, err)
	}

	if !bytes.Equal(value, []byte("Great")) {
		t.Errorf(`Unexpected value for key "party": got %q, want %q`, value, "Great")
	}
}
