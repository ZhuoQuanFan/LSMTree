package wal

import (
	"encoding/json"
	"os"
	"sync"
)

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WAL struct {
	file  *os.File
	mutex sync.Mutex
}

func NewWAL(filename string) (*WAL, error) {
	if file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return nil, err
	} else {
		return &WAL{file: file}, nil
	}
}

// TODO：用easyjson代替json
func (w *WAL) Write(key, value string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	entry := Entry{
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.file.Close()
}

func RecoverWAL(filename string) (map[string]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), nil
		}
		return nil, err
	}
	defer file.Close()

	result := make(map[string]string)
	decoder := json.NewDecoder(file)
	for {
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			if err := decoder.Decode(&entry); err != nil {
				break
			}
			result[entry.Key] = entry.Value
		}
	}
	return result, nil
}
