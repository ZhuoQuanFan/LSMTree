package main

import (
	"fmt"
	"os"
	"src/lsm-tree/lsm"
)

func main() {
	filepath := "./data"
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath, 0777); err != nil {
			panic(err)
		}
	}
	lsmTree, err := lsm.NewLSMTree("./data", 10)
	if err != nil {
		panic(err)
	}
	defer lsmTree.Close()

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := lsmTree.Put(key, value); err != nil {
			panic(err)
		}
	}
	fmt.Println("Successfully inserted keys")

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key%d", i)
		if value, ok := lsmTree.Get(key); ok {
			fmt.Printf("%s: %s\n", key, value)
		} else {
			fmt.Printf("%s not found\n", key)
		}
	}

	// 执行合并
	if err := lsmTree.Compact(); err != nil {
		panic(err)
	}

	// 再次查询验证
	if value, ok := lsmTree.Get("key0"); ok {
		fmt.Printf("After compaction - key0: %s\n", value)
	}
}
