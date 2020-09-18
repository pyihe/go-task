# go-task

#### 简介

基于[Etcd](https://github.com/etcd-io/etcd)实现的定时任务，每个任务只会被一个节点执行，任务执行时间借用[Cron](https://github.com/robfig/cron)的Parser解析。

#### 包含接口

|API|Comment|
|:---|:---|
|RegisterHandler|注册任务Key的Handler(每个key只能注册一次)|
|RegisterHandlerWithCover|注册任务Key的Handler(可以覆盖之前注册的Handler)|
|Add|添加任务|
|AddWithCover|添加任务(可覆盖)|
|UpdateTime|更新任务执行时间|
|Remove|移除任务|

#### 使用

```go
package main

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	go_task "github.com/pyihe/go-task"
	"time"
)

func main() {
	config := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(config)
	if err != nil {
		fmt.Printf("new err: %v\n", err)
		return
	}
	defer c.Close()

	tsker := go_task.NewTasker(c, "task", 10)

	if err = tsker.RegisterHandler("key1", handler); err != nil {
		//handle(err)
	}

	if err = tsker.Add("key1", "value1", "@every 1m", go_task.TaskTypeOnceCall); err != nil {
		//handle(err)
	}

	if err = tsker.UpdateTime("key1", "value1", "@every 10m"); err != nil {
		//handle(err)
	}

	//remove the task with specified key and value
	if err = tsker.Remove("key1", "value1"); err != nil {
		//handle(err)
	}
}

func handler(key, value string) {
	//handle with key and value
}
```