package badgerstorage

import (
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
	tests.NewTestSuite(New(db)).Run(t)
}
