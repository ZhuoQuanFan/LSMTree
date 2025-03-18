package sstable

import (
	"os"
	"testing"
)

func TestBloomFilterPersistence(t *testing.T) {
	// 使用临时目录避免干扰实际数据
	tempDir := t.TempDir()
	filepath := tempDir + "/test_sstable.sst"

	// 测试用例 1: 单个键的保存和加载
	t.Run("SingleKey", func(t *testing.T) {
		// 初始化并添加键
		sst := NewSSTable(filepath)
		sst.bloom.AddString("key1")
		data := map[string]string{"key1": "value1"}
		if err := sst.Write(data); err != nil {
			t.Fatalf("Failed to write SSTable: %v", err)
		}

		// 重新加载
		sst2 := NewSSTable(filepath)
		if sst2.bloom == nil {
			t.Fatal("Bloom filter not initialized after load")
		}

		// 验证加载后的布隆过滤器
		if !sst2.bloom.TestString("key1") {
			t.Error("Loaded bloom filter failed to detect existing key 'key1'")
		}
		if sst2.bloom.TestString("key2") {
			t.Error("Loaded bloom filter incorrectly detected non-existent key 'key2'")
		}
	})

	// 测试用例 2: 多个键的保存和加载
	t.Run("MultipleKeys", func(t *testing.T) {
		// 初始化并添加多个键
		sst := NewSSTable(filepath)
		keys := []string{"key1", "key2", "key3"}
		for _, key := range keys {
			sst.bloom.AddString(key)
		}
		data := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}
		if err := sst.Write(data); err != nil {
			t.Fatalf("Failed to write SSTable: %v", err)
		}

		// 重新加载
		sst2 := NewSSTable(filepath)
		if sst2.bloom == nil {
			t.Fatal("Bloom filter not initialized after load")
		}

		// 验证所有键
		for _, key := range keys {
			if !sst2.bloom.TestString(key) {
				t.Errorf("Loaded bloom filter failed to detect existing key '%s'", key)
			}
		}
		if sst2.bloom.TestString("key4") {
			t.Error("Loaded bloom filter incorrectly detected non-existent key 'key4'")
		}
	})

	// 测试用例 3: 文件删除后重新创建
	t.Run("FileDeleted", func(t *testing.T) {
		// 删除文件
		if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
			t.Fatalf("Failed to remove SSTable file: %v", err)
		}
		if err := os.Remove(filepath + ".bloom"); err != nil && !os.IsNotExist(err) {
			t.Fatalf("Failed to remove bloom file: %v", err)
		}

		// 重新创建
		sst := NewSSTable(filepath)
		if sst.bloom == nil {
			t.Fatal("Bloom filter not initialized after file deletion")
		}
		if sst.bloom.TestString("key1") {
			t.Error("New bloom filter incorrectly detected non-existent key 'key1'")
		}
	})

	// 测试用例 4: 边界情况 - 空布隆过滤器
	t.Run("EmptyBloom", func(t *testing.T) {
		sst := NewSSTable(filepath)
		data := make(map[string]string)
		if err := sst.Write(data); err != nil {
			t.Fatalf("Failed to write empty SSTable: %v", err)
		}

		sst2 := NewSSTable(filepath)
		if sst2.bloom == nil {
			t.Fatal("Bloom filter not initialized for empty case")
		}
		if sst2.bloom.TestString("key1") {
			t.Error("Empty bloom filter incorrectly detected non-existent key 'key1'")
		}
	})
}

func TestMain(m *testing.M) {
	// 运行测试
	code := m.Run()
	os.Exit(code)
}
