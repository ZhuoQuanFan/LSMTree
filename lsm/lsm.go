package lsm

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"src/lsm-tree/skiplist"
	"src/lsm-tree/sstable"
	"src/lsm-tree/wal"
	"sync"
	"time"
)

type LSMTree struct {
	memTable    *skiplist.SkipList
	wal         *wal.WAL
	sstables    []*sstable.SSTable
	maxSize     int
	baseDir     string
	mutex       sync.Mutex
	sstableSeq  int
	flushChan   chan struct{}
	compactChan chan struct{}
	wg          sync.WaitGroup
	closed      bool
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

	lsm := &LSMTree{
		memTable:    memTable,
		wal:         walInstance,
		sstables:    make([]*sstable.SSTable, 0),
		maxSize:     maxSize,
		baseDir:     baseDir,
		sstableSeq:  0,
		flushChan:   make(chan struct{}, 1),
		compactChan: make(chan struct{}, 1),
	}

	lsm.wg.Add(1)
	go lsm.backgroundWorker()

	return lsm, nil
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

	//判断是否需要合并SSTable文件
	if len(lsm.sstables) >= 3 {
		select {
		case lsm.compactChan <- struct{}{}:
		default:
		}
	}
	return nil

}
func (lsm *LSMTree) Put(key, value string) error {
	lsm.mutex.Lock()
	defer lsm.mutex.Unlock()

	if lsm.closed {
		return fmt.Errorf("lsm tree is closed")
	}
	//写入WAL
	if err := lsm.wal.Write(key, value); err != nil {
		return err
	}

	//写入MemTable
	lsm.memTable.Put(key, value)

	if lsm.memTable.Size() >= lsm.maxSize {
		select {
		case lsm.flushChan <- struct{}{}:
		default:
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

func (lsm *LSMTree) backgroundWorker() {
	defer lsm.wg.Done()

	for {
		select {
		case <-lsm.flushChan:
			// 加锁
			lsm.mutex.Lock()
			if !lsm.closed && lsm.memTable.Size() > 0 {
				// 尝试刷新内存表
				if err := lsm.flush(); err != nil {
					log.Printf("Flush error: %v", err)
				}
			}
			// 解锁
			lsm.mutex.Unlock()
		case <-lsm.compactChan:
			// 加锁
			lsm.mutex.Lock()
			if !lsm.closed && len(lsm.sstables) > 1 {
				// 尝试合并SSTables
				if err := lsm.Compact(); err != nil {
					log.Printf("Compaction error: %v", err)
				}
			}
		case <-time.After(time.Second * 10):
			// 加锁
			lsm.mutex.Lock()
			if !lsm.closed && len(lsm.sstables) >= 3 {
				// 尝试合并SSTables
				if err := lsm.Compact(); err != nil {
					log.Printf("Compaction error: %v", err)
				}
			}
			// 解锁
			lsm.mutex.Unlock()
		}

		// 加锁
		lsm.mutex.Lock()

		if lsm.closed {
			// 解锁
			lsm.mutex.Unlock()
			return
		}
		// 解锁
		lsm.mutex.Unlock()
	}
}
