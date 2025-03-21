package sstable

import (
	"fmt"
	"math"
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

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// 测试参数
	const (
		n       = 10000   // 插入元素数量
		fpRate  = 0.01    // 目标误判率
		testNum = 1000000 // 测试查询次数
	)

	// 使用临时目录
	tempDir := t.TempDir()
	filepath := tempDir + "/test_sstable.sst"

	// 创建布隆过滤器并插入数据
	sst := NewSSTable(filepath)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		sst.bloom.AddString(key)
	}

	// 保存到磁盘
	data := make(map[string]string)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		data[key] = fmt.Sprintf("value%d", i)
	}
	if err := sst.Write(data); err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	// 重新加载布隆过滤器
	sst2 := NewSSTable(filepath)
	if sst2.bloom == nil {
		t.Fatal("Bloom filter not initialized after load")
	}

	// 验证已插入的键都能检测到
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		if !sst2.bloom.TestString(key) {
			t.Errorf("Failed to detect existing key '%s'", key)
		}
	}

	// 测试误判率：查询不在集合中的键
	falsePositives := 0
	for i := 0; i < testNum; i++ {
		key := fmt.Sprintf("test%d", i) // 确保这些键不在集合中
		if sst2.bloom.TestString(key) {
			falsePositives++
		}
	}

	// 计算实际误判率
	measuredFpRate := float64(falsePositives) / float64(testNum)
	t.Logf("Measured false positive rate: %.4f", measuredFpRate)

	// 计算理论误判率
	m := float64(sst2.bloom.Cap())
	k := float64(sst2.bloom.K())
	theoreticalFpRate := math.Pow(1-math.Exp(-k*float64(n)/m), k)
	t.Logf("Theoretical false positive rate: %.4f", theoreticalFpRate)

	// 验证实际误判率是否接近理论值（允许一定偏差，例如 20%）
	if math.Abs(measuredFpRate-theoreticalFpRate)/theoreticalFpRate > 0.2 {
		t.Errorf("Measured false positive rate %.4f deviates too much from theoretical %.4f", measuredFpRate, theoreticalFpRate)
	}

	// 验证实际误判率是否接近目标误判率
	if math.Abs(measuredFpRate-fpRate)/fpRate > 0.2 {
		t.Errorf("Measured false positive rate %.4f deviates too much from target %.4f", measuredFpRate, fpRate)
	}
}

func TestMain(m *testing.M) {
	// 运行测试
	code := m.Run()
	os.Exit(code)
}
