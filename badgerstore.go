package raftbadger

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

var (
	ErrKeyNotFound = errors.New("not found")
)

type LogStableStore interface {
	raft.LogStore
	raft.StableStore
}

type BadgerStore struct {
	db *badger.DB
}

func New(opt badger.Options) (*BadgerStore, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	s := &BadgerStore{
		db: db,
	}
	return s, nil
}

func (s *BadgerStore) Close() error                { return s.db.Close() }
func (s *BadgerStore) FirstIndex() (uint64, error) { return s.firstIndex(false) }
func (s *BadgerStore) LastIndex() (uint64, error)  { return s.firstIndex(true) }

func (s *BadgerStore) GetLog(idx uint64, log *raft.Log) error {
	var item *badger.Item
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		item, err = txn.Get(prefixKey(uint64ToBytes(idx), "logs"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	v, err := item.Value()
	if err != nil {
		return err
	}
	return decodeMsgPack(v, log)
}

func (s *BadgerStore) StoreLog(log *raft.Log) error {
	buf, err := encodeMsgPack(log)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(prefixKey(uint64ToBytes(log.Index), "logs"), buf.Bytes(), 0)
	})
}

func (s *BadgerStore) StoreLogs(logs []*raft.Log) error {
	txn := s.db.NewTransaction(true)
	for _, log := range logs {
		buf, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		txn.Set(prefixKey(uint64ToBytes(log.Index), "logs"), buf.Bytes(), 0)
	}
	return txn.Commit(nil)
}

func (s *BadgerStore) DeleteRange(min, max uint64) error {
	dTxn := s.db.NewTransaction(true)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		for it.Seek(uint64ToBytes(min)); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytesToUint64(key) > max {
				break
			}
			dTxn.Delete(prefixKey(key, "logs"))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return dTxn.Commit(nil)
}

func (s *BadgerStore) Get(k []byte) ([]byte, error) {
	var item *badger.Item
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		item, err = txn.Get(prefixKey(k, "conf"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return item.Value()
}

func (s *BadgerStore) Set(k, v []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(prefixKey(k, "conf"), v, 0)
	})
}

func (s *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (s *BadgerStore) SetUint64(key []byte, val uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, uint64ToBytes(val), 0)
	})
}

func (s *BadgerStore) firstIndex(reverse bool) (uint64, error) {
	var item *badger.Item
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchSize:   1,
			PrefetchValues: false,
			Reverse:        reverse,
		})
		prefix := []byte("logs")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item = it.Item()
			fmt.Printf("firstindex - item = %+v\n", item)
			return nil
		}
		return badger.ErrKeyNotFound
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	return bytesToUint64(item.Key()), nil
}
