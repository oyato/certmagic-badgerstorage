// Package badgerstorage implements certmagic.Storage on top of a Badger database.
//
// It's an alternative to the default file-system storage used by CertMagic.
package badgerstorage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/caddyserver/certmagic"
	"github.com/dgraph-io/badger/v2"
	"io/fs"
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
func (sto *Storage) Lock(_ context.Context, key string) error {
	sto.ls.Lock(key)
	return nil
}

// Unlock implements certmagic.Storage.Unlock
func (sto *Storage) Unlock(_ context.Context, key string) error {
	return sto.ls.TryUnlock(key)
}

// Store implements certmagic.Storage.Store
func (sto *Storage) Store(_ context.Context, key string, value []byte) error {
	err := sto.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("Storage.Store: %w", err)
	}
	return nil
}

// Load implements certmagic.Storage.Load
func (sto *Storage) Load(_ context.Context, key string) ([]byte, error) {
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
		return nil, fs.ErrNotExist
	}
	return val, nil
}

// Delete implements certmagic.Storage.Delete
func (sto *Storage) Delete(_ context.Context, key string) error {
	err := sto.DB.Update(func(txn *badger.Txn) error {
		k := []byte(key)
		return txn.Delete(k)
	})
	if err != nil {
		return fmt.Errorf("Storage.Delete: %w", err)
	}
	return nil
}

// Exists implements certmagic.Storage.Exists
func (sto *Storage) Exists(_ context.Context, key string) bool {
	err := sto.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
	return err == nil
}

// List implements certmagic.Storage.List
func (sto *Storage) List(_ context.Context, prefix string, recursive bool) ([]string, error) {
	seen := map[string]bool{}
	var keys []string
	err := sto.DB.View(func(txn *badger.Txn) error {
		pfx := make([]byte, 0, len(prefix)+1)
		pfx = append(pfx, prefix...)
		pfx = append(pfx, '/')
		it := txn.NewIterator(badger.IteratorOptions{Prefix: pfx})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			return badger.ErrKeyNotFound
		}
		for ; it.Valid(); it.Next() {
			walkKey(it.Item().Key(), len(pfx), recursive, func(k []byte) {
				if seen[string(k)] {
					return
				}
				seen[string(k)] = true
				keys = append(keys, string(k))
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Storage.List: %w", err)
	}
	return keys, nil
}

// Stat implements certmagic.Storage.Stat
func (sto *Storage) Stat(_ context.Context, key string) (certmagic.KeyInfo, error) {
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

func walkKey(k []byte, sp int, recursive bool, f func([]byte)) {
	if sp >= len(k) {
		return
	}
	if i := bytes.IndexByte(k[sp:], '/'); i >= 0 {
		sp += i
	} else {
		sp = len(k)
	}
	f(k[:sp])
	if recursive {
		walkKey(k, sp+1, recursive, f)
	}
}
