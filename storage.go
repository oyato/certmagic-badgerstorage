// Package storage implements certmagic.Storage on top of a Badger database.
//
// It's an alternative to the default file-system storage used by CertMagic.
package storage

import (
	"bytes"
	"fmt"
	"github.com/caddyserver/certmagic"
	"github.com/dgraph-io/badger/v2"
	"oya.to/namedlocker"
)

var (
	_ certmagic.Storage = (*Storage)(nil)
)

// Storage implements certmagic.Storage
type Storage struct {
	// DB is the underlying badger database
	DB *badger.DB

	ls namedlocker.Store
}

// Lock implements certmagic.Storage.Lock
func (sto *Storage) Lock(key string) error {
	sto.ls.Lock(key)
	return nil
}

// Unlock implements certmagic.Storage.Unlock
func (sto *Storage) Unlock(key string) error {
	return sto.ls.TryUnlock(key)
}

// Store implements certmagic.Storage.Store
func (sto *Storage) Store(key string, value []byte) error {
	err := sto.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("Storage.Store: %w", err)
	}
	return nil
}

// Load implements certmagic.Storage.Load
func (sto *Storage) Load(key string) ([]byte, error) {
	var val []byte
	err := sto.DB.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return itm.Value(func(v []byte) error {
			val = append([]byte(nil), v...)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("Storage.Load: %w", err)
	}
	return val, nil
}

// Delete implements certmagic.Storage.Delete
func (sto *Storage) Delete(key string) error {
	err := sto.DB.Update(func(txn *badger.Txn) error {
		k := []byte(key)
		if _, err := txn.Get(k); err == badger.ErrKeyNotFound {
			return err
		}
		return txn.Delete(k)
	})
	if err != nil {
		return fmt.Errorf("Storage.Delete: %w", err)
	}
	return nil
}

// Exists implements certmagic.Storage.Exists
func (sto *Storage) Exists(key string) bool {
	err := sto.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
	return err == nil
}

// List implements certmagic.Storage.List
func (sto *Storage) List(prefix string, recursive bool) ([]string, error) {
	var keys []string
	err := sto.DB.View(func(txn *badger.Txn) error {
		dir := make([]byte, 0, len(prefix)+1)
		dir = append(dir, prefix...)
		dir = append(dir, '/')
		it := txn.NewIterator(badger.IteratorOptions{Prefix: dir})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			return badger.ErrKeyNotFound
		}
		for ; it.Valid(); it.Next() {
			itm := it.Item()
			key := itm.Key()
			fn := bytes.TrimPrefix(key, dir)
			if len(fn) != 0 && (recursive || !bytes.Contains(fn, []byte{'/'})) {
				keys = append(keys, string(key))
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Storage.List: %w", err)
	}
	return keys, nil
}

// Stat implements certmagic.Storage.Stat
func (sto *Storage) Stat(key string) (certmagic.KeyInfo, error) {
	inf := certmagic.KeyInfo{Key: key}
	err := sto.DB.View(func(txn *badger.Txn) error {
		fn := make([]byte, 0, len(key)+1)
		fn = append(fn, key...)

		// if it exists, it must be a "file"
		itm, err := txn.Get(fn)
		if err == nil {
			inf.IsTerminal = true
			// itm.ValueSize() is only an "estimate",
			// but certmagic - at this time - seems to only use it for sorting, so it's ok.
			inf.Size = itm.ValueSize()
			return nil
		}

		// otherwise, look for a "directory"
		dir := append(fn, '/')
		it := txn.NewIterator(badger.IteratorOptions{Prefix: dir})
		defer it.Close()
		if it.Rewind(); !it.ValidForPrefix(dir) {
			return badger.ErrKeyNotFound
		}
		return nil
	})
	if err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("Storage.Stat: %w", err)
	}
	return inf, nil
}

// New return a new Storage using db to store persisted data.
func New(db *badger.DB) *Storage {
	return &Storage{DB: db}
}
