package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()

	// 注册测试路由
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"version":"test"}`)
	})

	mux.HandleFunc("/sync_global", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"success":true,"message":"sync_global route works"}`)
	})

	fmt.Println("测试服务器启动在 :9191")
	fmt.Println("测试命令:")
	fmt.Println("curl http://127.0.0.1:9191/version")
	fmt.Println("curl -X POST http://127.0.0.1:9191/sync_global")

	log.Fatal(http.ListenAndServe(":9191", mux))
}
