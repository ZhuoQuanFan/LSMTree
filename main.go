package main

import (
	"fmt"
	"net/http"
	"os"
	"LSMTree/lsm"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// 定义API请求和响应结构体
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
}

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
	defer func() {
		if closeErr := lsmTree.Close(); closeErr != nil {
			fmt.Printf("Error closing LSM tree: %v\n", closeErr)
		}
	}()

	// 初始化 Echo 实例
	e := echo.New()

	// 中间件
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS()) // 允许跨域请求

	// 静态文件服务 (用于前端HTML/CSS/JS)
	e.Static("/", "public") // 假设前端文件在 `public` 目录下

	// API 路由
	e.POST("/put", func(c echo.Context) error {
		req := new(PutRequest)
		if err := c.Bind(req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		if err := lsmTree.Put(req.Key, req.Value); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, map[string]string{"message": "success"})
	})

	e.GET("/get/:key", func(c echo.Context) error {
		key := c.Param("key")
		value, ok := lsmTree.Get(key)
		return c.JSON(http.StatusOK, GetResponse{Key: key, Value: value, Found: ok})
	})

	e.POST("/compact", func(c echo.Context) error {
		if err := lsmTree.Compact(); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, map[string]string{"message": "compaction started"})
	})

	fmt.Println("LSM-Tree server starting on :8080")
	// 启动服务器
	e.Logger.Fatal(e.Start(":8080"))

	// 以下是原有的测试代码，可以保留或移除，但建议在Web服务启动后不再自动执行
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			if err := lsmTree.Put(key, value); err != nil {
				fmt.Printf("Put error for %s: %v\n", key, err)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("Successfully inserted initial keys (if any)")

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key%d", i)
		if value, ok := lsmTree.Get(key); ok {
			fmt.Printf("%s: %s\n", key, value)
		} else {
			fmt.Printf("%s not found\n", key)
		}
	}
}
