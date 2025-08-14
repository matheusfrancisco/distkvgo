package db

import (
	bolt "go.etcd.io/bbolt"
)

var (
	dbBucket = []byte("default")
)

type DB struct {
	db *bolt.DB
	readOnly bool
}

func New(path string, readOnly bool) (d *DB, err error) {
	database, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	d = &DB{db: database, readOnly: readOnly}

	if err := d.createDefaultBucket(); err != nil {
		d.db.Close()
		return nil, err
	}

	return d, nil
}

func (d *DB) createDefaultBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dbBucket)
		return err
	})
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
