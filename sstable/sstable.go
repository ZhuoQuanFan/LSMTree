package sstable

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
)

type SSTable struct {
	filepath string
	index    map[string]int64
	mutex    sync.RWMutex
}

func NewSSTable(filepath string) *SSTable {
	return &SSTable{
		filepath: filepath,
		index:    make(map[string]int64),
	}
}

func (sst *SSTable) GetFilePath() string {
	return sst.filepath
}
func (s *SSTable) Write(data map[string]string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	file, err := os.Create(s.filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	offset := int64(0)
	for _, key := range keys {
		entry := struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{Key: key, Value: data[key]}

		data, err := json.Marshal(entry)

		if err != nil {
			return err
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			return err
		}
		s.index[key] = offset

		offset += int64(len(data) + 1)
	}
	return file.Sync()
}

func (s *SSTable) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	offset, exists := s.index[key]
	if !exists {
		return "", false
	}

	file, err := os.Open(s.filepath)
	if err != nil {
		return "", false
	}
	defer file.Close()

	if _, err := file.Seek(offset, 0); err != nil {
		return "", false
	}

	var entry struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&entry); err != nil {
		return "", false
	}
	return entry.Value, true

}
