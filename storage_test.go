package badgerstorage

import (
	"fmt"
	"github.com/caddyserver/certmagic"
	"github.com/dgraph-io/badger/v2"
	tests "github.com/oyato/certmagic-storage-tests"
	"log"
	"testing"
)

func Example() {
	// setup the badger DB
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		log.Fatalf("Cannot open badger memory DB: %s", err)
	}

	// set the default CertMagic storage to replace the file-system based on.
	certmagic.Default.Storage = New(db)

	// setup the rest of your CertMagic stuff...
}

func TestStorage(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatalf("Cannot open badger memory DB: %s", err)
	}
	sto := New(db)
	tests.NewTestSuite(sto).Run(t)
	if err := sto.Delete(""); err == nil {
		t.Fatalf("Storage.Delete with empty key should fail")
	}
}

func TestWalkKey(t *testing.T) {
	pfx := "dir/"
	tbl := []struct {
		rec bool
		key string
		exp []string
	}{
		{false, "", []string{}},
		{false, "a/1/2", []string{"a"}},
		{false, "b/3", []string{"b"}},
		{false, "c", []string{"c"}},
		{true, "", []string{}},
		{true, "a/1/2", []string{"a", "a/1", "a/1/2"}},
		{true, "b/3", []string{"b", "b/3"}},
		{true, "c", []string{"c"}},
	}
	for _, tst := range tbl {
		if tst.key != "" {
			tst.key = pfx + tst.key
		}
		for i, s := range tst.exp {
			tst.exp[i] = pfx + s
		}

		ls := []string{}
		walkKey([]byte(tst.key), len(pfx), tst.rec, func(k []byte) {
			ls = append(ls, string(k))
		})
		got := fmt.Sprintf("%#q", ls)
		exp := fmt.Sprintf("%#q", tst.exp)
		if got != exp {
			t.Errorf("walkKey(%#q, %d, %v): should return %s, not %s",
				tst.key, len(pfx), tst.rec, exp, got,
			)
		}
	}
}
