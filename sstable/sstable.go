package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type SSTable struct {
	filepath string
	index    map[string]int64
	mutex    sync.RWMutex
	bloom    *bloom.BloomFilter
}

//easyjson:json
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewSSTable(filepath string) *SSTable {
	sst := &SSTable{
		filepath: filepath,
		index:    make(map[string]int64),
	}
	// 检查是否存在布隆过滤器文件
	bloomFile := filepath + ".bloom"
	if _, err := os.Stat(bloomFile); err == nil {
		file, err := os.Open(bloomFile)
		if err == nil {
			defer file.Close()

			// 读取元数据 (m, k)
			var m, k uint32
			if err := binary.Read(file, binary.LittleEndian, &m); err != nil {
				fmt.Printf("Failed to read m from %s: %v\n", bloomFile, err)
				return &SSTable{filepath: filepath, index: make(map[string]int64), bloom: bloom.NewWithEstimates(10000, 0.01)}
			}
			if err := binary.Read(file, binary.LittleEndian, &k); err != nil {
				fmt.Printf("Failed to read k from %s: %v\n", bloomFile, err)
				return &SSTable{filepath: filepath, index: make(map[string]int64), bloom: bloom.NewWithEstimates(10000, 0.01)}
			}

			// 读取位数组数据
			data := make([]byte, m/8) // 转换为字节数
			if _, err := io.ReadFull(file, data); err != nil {
				fmt.Printf("Failed to read bloom data from %s: %v\n", bloomFile, err)
				return &SSTable{filepath: filepath, index: make(map[string]int64), bloom: bloom.NewWithEstimates(10000, 0.01)}
			}

			// 转换为 uint64 切片
			uint64Data := make([]uint64, (len(data)+7)/8) // 确保足够空间
			for i := 0; i < len(data); i += 8 {
				end := i + 8
				if end > len(data) {
					end = len(data)
				}
				padding := make([]byte, 8-len(data[i:end])) // 修正：移除多余的 ...
				val := binary.LittleEndian.Uint64(append(data[i:end], padding...))
				uint64Data[i/8] = val
			}

			// 创建布隆过滤器
			sst.bloom = bloom.FromWithM(uint64Data, uint(m), uint(k))
			return sst
		}
	}
	sst.bloom = bloom.NewWithEstimates(10000, 0.01) // 10000 元素，1% 误判率
	return sst

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
		//easyjson:json
		entry := Entry{Key: key, Value: data[key]}

		data, err := entry.MarshalJSON()

		if err != nil {
			return err
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			return err
		}
		s.index[key] = offset
		s.bloom.AddString(key)
		offset += int64(len(data) + 1)
	}
	return file.Sync()
}

func (s *SSTable) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.bloom.TestString(key) {
		return "", false
	}
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
