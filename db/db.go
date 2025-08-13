package db

import (
	"log"

	bolt "go.etcd.io/bbolt"
)

var (
	dbBucket = []byte("default")
)

type DB struct {
	db *bolt.DB
}

func New(path string) (db *DB, err error) {
	database, err := bolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &DB{db: database}, nil
}

func (d *DB) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		return b.Put([]byte(key), value)
	})
}

func (d *DB) GetKey(key string) ([]byte, error) {
	var value []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket)
		value = b.Get([]byte(key))
		return nil
	})
	if err == nil {
		return value, nil
	}
	return nil, err
}

func (d *DB) Close() error {
	return d.db.Close()
}
