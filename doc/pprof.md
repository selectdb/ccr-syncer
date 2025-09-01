# pprof使用介绍

## pprof简介
pprof是golang语言中，用来分析性能的工具，pprof有4种profling：
1. CPU Profiling : CPU 性能分析
2. memory Profiling : 程序的内存占用情况
3. Block Profiling :  goroutine 在等待共享资源花费的时间
4. Mutex Profiling : 只记录因为锁竞争导致的等待或延迟
目前CCR已经集成了pprof，可以用来分析CCR的性能。

## CCR中使用pprof的步骤
1. 启动CCR进程时，可以通过sh shell/start_syncer.sh --pprof true --pprof_port 8080 --host x.x.x.x --daemon的方式打开pprof
2. 在浏览器中打开 http://x.x.x.x:8080/debug/pprof/ 即可看到profiling
3. 或者可以使用采样工具，通过更加图形化的方式来分析，此时可以在8080端口启动后，在ccr机器上执行
``` go tool pprof -http=:9999 http://x.x.x.x:8080/debug/pprof/heap ```
然后在浏览器打开 http://x.x.x.x:9999 即可看到采样图形化信息  
此处需要注意的是，如果无法开通端口，可以使用如下命令将采样信息保存到文件中，再将文件拉到本地使用浏览器打开：
``` curl http://localhost:8080/debug/pprof/heap?seconds=30 > heap.out ```
``` go tool pprof heap.out ```