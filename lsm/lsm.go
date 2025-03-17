package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"src/lsm-tree/skiplist"
	"src/lsm-tree/sstable"
	"src/lsm-tree/wal"
	"sync"
)

type LSMTree struct {
	memTable   *skiplist.SkipList
	wal        *wal.WAL
	sstables   []*sstable.SSTable
	maxSize    int
	baseDir    string
	mutex      sync.Mutex
	sstableSeq int
}

func NewLSMTree(baseDir string, maxSize int) (*LSMTree, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	walFile := fmt.Sprintf("%s/wal.log", baseDir)
	walInstance, err := wal.NewWAL(walFile)
	if err != nil {
		return nil, err
	}

	recovered, err := wal.RecoverWAL(walFile)
	if err != nil {
		return nil, err
	}

	memTable := skiplist.NewSkipList(16)
	for k, v := range recovered {
		memTable.Put(k, v)
	}

	return &LSMTree{
		memTable:   memTable,
		wal:        walInstance,
		sstables:   make([]*sstable.SSTable, 0),
		maxSize:    maxSize,
		baseDir:    baseDir,
		sstableSeq: 0,
	}, nil
}
func (lsm *LSMTree) flush() error {
	sstableFile := fmt.Sprintf("%s/sstable-%d", lsm.baseDir, lsm.sstableSeq)
	sst := sstable.NewSSTable(sstableFile)
	if err := sst.Write(lsm.memTable.ToMap()); err != nil {
		return err
	}
	lsm.sstables = append(lsm.sstables, sst)

	lsm.sstableSeq++

	lsm.memTable = skiplist.NewSkipList(16)

	if err := lsm.wal.Close(); err != nil {
		return err
	}

	walFile := fmt.Sprintf("%s/wal.log", lsm.baseDir)
	if err := os.Remove(walFile); err != nil {
		return err
	}

	walInstance, err := wal.NewWAL(walFile)
	if err != nil {
		return err
	}

	lsm.wal = walInstance
	return nil

}
func (lsm *LSMTree) Put(key, value string) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	//写入WAL
	if err := lsm.wal.Write(key, value); err != nil {
		return err
	}

	//写入MemTable
	lsm.memTable.Put(key, value)

	if lsm.memTable.Size() >= lsm.maxSize {
		if err := lsm.flush(); err != nil {
			return err
		}
	}
	return nil

}

func (lsm *LSMTree) Get(Key string) (string, bool) {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if value, ok := lsm.memTable.Get(Key); ok {
		return value, true
	}

	for i := len(lsm.sstables) - 1; i >= 0; i-- {
		if value, ok := lsm.sstables[i].Get(Key); ok {
			return value, true
		}
	}
	return "", false
}

func (lsm *LSMTree) Compact() error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if len(lsm.sstables) < 2 {
		return nil
	}

	merged := make(map[string]string)

	for _, sst := range lsm.sstables {
		filepath := sst.GetFilePath()
		file, err := os.Open(filepath)
		if err != nil {
			return err
		}
		defer file.Close()

		decoder := json.NewDecoder(file)

		for {
			var entry struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if err := decoder.Decode(&entry); err != nil {
				break
			}
			merged[entry.Key] = entry.Value
		}
	}
	newSSTableFile := fmt.Sprintf("%s/sstable_%d.sst", lsm.baseDir, lsm.sstableSeq)
	newSST := sstable.NewSSTable(newSSTableFile)
	if err := newSST.Write(merged); err != nil {
		return err
	}

	for _, sst := range lsm.sstables {
		if err := os.Remove(sst.GetFilePath()); err != nil {
			return err
		}
	}

	lsm.sstables = []*sstable.SSTable{newSST}
	lsm.sstableSeq++
	return nil
}

func (lsm *LSMTree) Close() error {
	if err := lsm.flush(); err != nil {
		return err
	}
	return lsm.wal.Close()
}
