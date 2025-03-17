package main

import (
	"fmt"
	"os"
	"src/lsm-tree/lsm"
	"sync"
)

func main() {
	filepath := "./data"
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath, 0777); err != nil {
			panic(err)
		}
	}
	lsmTree, err := lsm.NewLSMTree("./data", 11)
	if err != nil {
		panic(err)
	}
	defer lsmTree.Close()

	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			if err := lsmTree.Put(key, value); err != nil {
				panic(err)
			}
		}(i)

	}
	wg.Wait()
	fmt.Println("Successfully inserted keys")
	fmt.Println(lsmTree)

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
