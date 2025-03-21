package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

//easyjson:json
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SSTable struct {
	filepath string
	index    map[string]int64
	mutex    sync.RWMutex
	bloom    *bloom.BloomFilter
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

			// 读取整个文件剩余内容
			data, err := io.ReadAll(file)
			if err != nil {
				fmt.Printf("Failed to read bloom data from %s: %v\n", bloomFile, err)
				return &SSTable{filepath: filepath, index: make(map[string]int64), bloom: bloom.NewWithEstimates(10000, 0.01)}
			}
			fmt.Printf("Read data length: %d, expected m/8: %d\n", len(data), m/8)

			// 创建一个新的布隆过滤器并加载数据
			bf := bloom.NewWithEstimates(uint(m), 0.01)
			if err := bf.UnmarshalBinary(data); err != nil {
				fmt.Printf("Failed to unmarshal bloom filter from %s: %v\n", bloomFile, err)
				return &SSTable{filepath: filepath, index: make(map[string]int64), bloom: bloom.NewWithEstimates(10000, 0.01)}
			}
			sst.bloom = bf
			return sst
		}
	}

	// 如果文件不存在或加载失败，创建新的布隆过滤器
	sst.bloom = bloom.NewWithEstimates(10000, 0.01)
	return sst
}

func (s *SSTable) Write(data map[string]string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 写入 SSTable 文件
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
		entry := Entry{Key: key, Value: data[key]}
		jsonData, err := entry.MarshalJSON()
		if err != nil {
			return err
		}
		if _, err := file.Write(append(jsonData, '\n')); err != nil {
			return err
		}
		s.index[key] = offset
		s.bloom.AddString(key)
		offset += int64(len(jsonData) + 1)
	}
	if err := file.Sync(); err != nil {
		return err
	}

	// 保存布隆过滤器到磁盘
	bloomFile := s.filepath + ".bloom"
	bloomOut, err := os.Create(bloomFile)
	if err != nil {
		return err
	}
	defer bloomOut.Close()

	// 保存元数据 (m, k)
	m := s.bloom.Cap()
	k := s.bloom.K()
	if err := binary.Write(bloomOut, binary.LittleEndian, uint32(m)); err != nil {
		return err
	}
	if err := binary.Write(bloomOut, binary.LittleEndian, uint32(k)); err != nil {
		return err
	}

	// 获取位数组并保存
	dataBytes, err := s.bloom.MarshalBinary()
	if err != nil {
		return err
	}
	fmt.Printf("Marshaled data length: %d, m/8: %d\n", len(dataBytes), m/8)
	if n, err := bloomOut.Write(dataBytes); err != nil || n != len(dataBytes) {
		return fmt.Errorf("failed to write bloom data: %v, wrote %d bytes, expected %d", err, n, len(dataBytes))
	}
	return bloomOut.Sync()
}

func (s *SSTable) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// 先检查布隆过滤器
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

	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil {
		return "", false
	}

	end := 0
	for i := 0; i < n; i++ {
		if buffer[i] == '\n' {
			end = i
			break
		}
	}

	var entry Entry
	if err := entry.UnmarshalJSON(buffer[:end]); err != nil {
		return "", false
	}
	return entry.Value, true
}

func (lsm *SSTable) GetFilePath() string {
	return lsm.filepath
}
