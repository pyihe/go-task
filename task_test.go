package go_task

import (
	"github.com/coreos/etcd/clientv3"
	"testing"
	"time"
)

func TestNewTasker(t *testing.T) {
	config := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(config)
	if err != nil {
		t.Fatalf("new etcd client err: %v\n", err)
	}
	tsKer := NewTasker(c, "task", 10)

	for i := 0; i < 3; i++ {
		go func(n int) {
			tsKer.RegisterHandler("key1", func(taskKey, taskValue string) {
				t.Logf("in gorutine: %d\n", n)
			})
		}(i + 1)
	}
	t.Log(tsKer.Add("key1", "value1", "50 20 11 9 9  3", TaskTypeOnceCall))
	select {}
}
