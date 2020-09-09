package go_task

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	cronV3 "github.com/robfig/cron/v3"
	"path"
	"strings"
	"sync"
	"time"
)

type Tasker interface {
	RegisterHandler(key string, handler TaskHandler) error
	RegisterHandlerWithCover(key string, handler TaskHandler) error
	Add(key, value, spec string, t TaskType) error
	AddWithCover(key, value, spec string, t TaskType) error
	UpdateTime(key, value string, spec string) error
	Remove(key, value string) error
}

const (
	TaskTypeOnceCall  = iota + 1 //单次执行
	TaskTypeMultiCall            //多次执行
)

const (
	taskStateNormal = iota + 1
	taskStateDel
)

type (
	TaskType    uint8
	TaskHandler func(taskKey, taskValue string)
)

type task struct {
	TaskType  TaskType `json:"task_type"`  //任务类型：单次执行和多次执行
	TaskKey   string   `json:"task_key"`   //任务对应的key
	TaskValue string   `json:"task_value"` //任务对应的value
	NextTime  string   `json:"next_time"`  //任务下次执行的时间：为了方便通过终端查看执行时间
	PreTime   string   `json:"pre_time"`   //任务上一次执行时的时间
	Spec      string   `json:"spec"`       //任务执行的spec
	Key       string   `json:"key"`        //etcd中的key
	State     uint8    `json:"state"`      //任务的状态
}

type eLocker struct {
	mu      sync.Mutex             //用于并发访问handler map
	runTime int64                  //正常情况下任务的执行时间
	prefix  string                 //用于创建etcd KV的前缀
	client  *clientv3.Client       //etcd客户端
	parser  cronV3.Parser          //借用cron的时间字符串解析器
	workers map[string]TaskHandler //每个key对应的worker
}

var (
	errNilHandler   = errors.New("register fail: nil handler")
	errExistHandler = errors.New("register fail: handler already register")
	errExistTask    = errors.New("add fail: task already exist")
	errNoTask       = errors.New("not exist task")
)

func NewTasker(client *clientv3.Client, prefix string, lockTimeOut int64) Tasker {
	if lockTimeOut <= 0 {
		panic("invalid task run time.")
	}
	if client == nil {
		panic("nil etcd client.")
	}
	if len(prefix) == 0 {
		panic("empty prefix.")
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	l := &eLocker{
		prefix:  prefix,
		client:  client,
		runTime: lockTimeOut,
		parser:  cronV3.NewParser(cronV3.Second | cronV3.Minute | cronV3.Hour | cronV3.Dom | cronV3.Month | cronV3.Dow | cronV3.Descriptor),
		workers: make(map[string]TaskHandler),
	}

	go l.watch()
	return l
}

//注册key对应的handler，不能重复注册
func (l *eLocker) RegisterHandler(key string, handler TaskHandler) error {
	if handler == nil {
		return errNilHandler
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.workers[key]; ok {
		return errExistHandler
	}

	l.workers[key] = handler
	return nil
}

//注册key对应的handler，可以重复注册
func (l *eLocker) RegisterHandlerWithCover(key string, handler TaskHandler) error {
	if handler == nil {
		return errNilHandler
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.workers[key] = handler

	return nil
}

//添加kv
func (l *eLocker) Add(key, value, spec string, tt TaskType) error {
	oldTask, err := l.getTask(context.Background(), key, value)
	if err != nil {
		return err
	}
	if oldTask != nil {
		return errExistTask
	}

	return l.AddWithCover(key, value, spec, tt)
}

//可以覆盖既有的KV
func (l *eLocker) AddWithCover(key, value, spec string, tt TaskType) error {
	nextTime, err := l.getNextTime(spec)
	if err != nil {
		return err
	}

	task := &task{
		TaskType:  tt,
		TaskKey:   key,
		TaskValue: value,
		NextTime:  nextTime.Format("2006-01-02 15:04:05"),
		PreTime:   "",
		Spec:      spec,
		Key:       path.Join(l.prefix, key, value),
		State:     taskStateNormal,
	}
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	leaseRsp, err := l.client.Grant(ctx, nextTime.Unix()-time.Now().Unix())
	if err != nil {
		return err
	}

	_, err = l.client.Put(ctx, task.Key, string(data), clientv3.WithLease(leaseRsp.ID))
	if err != nil {
		return err
	}
	return nil
}

func (l *eLocker) UpdateTime(key, value, spec string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	task, err := l.getTask(ctx, key, value)
	if err != nil {
		return err
	}
	if task == nil {
		return errNoTask
	}
	task.Spec = spec

	nextTime, err := l.getNextTime(spec)
	if err != nil {
		return err
	}
	leaseRsp, err := l.client.Grant(ctx, nextTime.Unix()-time.Now().Unix())
	if err != nil {
		return err
	}
	bytes, _ := json.Marshal(task)
	_, err = l.client.Put(ctx, task.Key, string(bytes), clientv3.WithLease(leaseRsp.ID))
	return err
}

func (l *eLocker) Remove(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	task, err := l.getTask(ctx, key, value)
	if err != nil {
		return err
	}
	if task == nil {
		return errNoTask
	}
	task.State = taskStateDel

	bytes, _ := json.Marshal(task)
	_, err = l.client.Put(ctx, task.Key, string(bytes))
	if err != nil {
		return err
	}

	_, err = l.client.Delete(context.Background(), task.Key)
	return err
}

func (l *eLocker) watch() {
	watchChan := l.client.Watch(context.Background(), l.prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for events := range watchChan {
		if err := events.Err(); err != nil {
			continue
		}
		for i := range events.Events {
			if events.Events[i].Type != mvccpb.DELETE {
				continue
			}
			bytes := events.Events[i].PrevKv.Value
			if len(bytes) == 0 {
				continue
			}
			var task *task
			if err := json.Unmarshal(bytes, &task); err != nil {
				continue
			}
			if task == nil {
				continue
			}
			if task.State == taskStateDel {
				continue
			}
			if task.TaskType == TaskTypeMultiCall {
				nextTime, err := l.getNextTime(task.Spec)
				if err != nil {
					continue
				}
				leaseRsp, err := l.client.Grant(context.Background(), nextTime.Unix()-time.Now().Unix())
				if err != nil {
					continue
				}
				task.PreTime = task.NextTime
				task.NextTime = nextTime.Format("2006-01-02 15:04:05")
				bytes, _ = json.Marshal(task)

				_, err = l.client.Put(context.Background(), task.Key, string(bytes), clientv3.WithLease(leaseRsp.ID))
				if err != nil {
					continue
				}
			}
			go l.run(task)
		}
	}
}

func (l *eLocker) run(t *task) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	leaseRsp, err := l.client.Grant(ctx, l.runTime)
	if err != nil {
		return
	}
	defer l.client.Revoke(ctx, leaseRsp.ID)
	//任务执行锁的key，用于保证只有一个实例或得锁
	runKey := path.Join(t.Key, t.NextTime)
	txnRsp, err := l.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(runKey), "=", 0)).
		Then(clientv3.OpPut(runKey, "", clientv3.WithLease(leaseRsp.ID))).Commit()
	if err != nil {
		return
	}
	if !txnRsp.Succeeded {
		return
	}

	l.mu.Lock()
	handler := l.workers[t.TaskKey]
	l.mu.Unlock()

	handler(t.TaskKey, t.TaskValue)

	if t.TaskType == TaskTypeOnceCall {
		l.Remove(t.TaskKey, t.TaskValue)
	}
}

func (l *eLocker) getNextTime(spec string) (time.Time, error) {
	s, err := l.parser.Parse(spec)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(time.Now()), nil
}

func (l *eLocker) getTask(ctx context.Context, key, value string) (*task, error) {
	k := path.Join(l.prefix, key, value)
	getRsp, err := l.client.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	var task *task
	if len(getRsp.Kvs) > 0 {
		err = json.Unmarshal(getRsp.Kvs[0].Value, &task)
	}
	return task, err
}
