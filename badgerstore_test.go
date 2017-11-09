package raftbadger_test

import (
	"log"
	"os"
	"testing"

	"github.com/dgraph-io/badger"

	"github.com/ChrisRx/raft-badger"
)

func init() {
	if err := os.MkdirAll("testdata", 0777); err != nil {
		log.Fatal(err)
	}
}

func TestBadgerStore(t *testing.T) {
	opt := badger.DefaultOptions
	opt.Dir = "testdata"
	opt.ValueDir = "testdata"
	_, err := raftbadger.New(opt)
	if err != nil {
		t.Fatal(err)
	}
}
