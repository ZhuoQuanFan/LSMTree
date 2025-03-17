package wal

import (
	"os"
	"sync"
)

//easyjson:json
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

func (w *WAL) Write(key, value string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	entry := Entry{
		Key:   key,
		Value: value,
	}

	data, err := entry.MarshalJSON()
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

	for {
		var entry Entry
		data := []byte{}
		if err := entry.UnmarshalJSON(data); err != nil {
			break
		} else {
			result[entry.Key] = entry.Value
		}

	}
	return result, nil
}
