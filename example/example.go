package main

import (
	"github.com/qit-team/work"
	"fmt"
	"context"
	"sync"
	"strconv"
	"time"
)

var (
	queues map[string][]string
	lock   sync.RWMutex
)

func init() {
	queues = make(map[string][]string)
}

func main() {
	stop := make(chan int, 0)
	q := new(LocalQueue)
	q2 := new(LocalQueue)

	job := work.New()
	job.AddQueue(q)
	job.AddQueue(q2, "kxy1")
	job.SetLogger(new(MyLogger))
	job.SetConsoleLevel(work.Info)
	job.SetEnableTopics("kxy1", "hts2")
	//task任务panic的回调函数
	job.RegisterTaskPanicCallback(func(task work.Task, e ...interface{}) {
		fmt.Println("task_panic_callback", task.Message)
		if len(e) > 0 {
			fmt.Println("task_panic_error", e[0])
		}
	})

	bench(job);

	//termStop(job);

	<-stop
}

//压测
func bench(job *work.Job) {
	RegisterWorkerBench(job)
	pushQueueData(job, "kxy1", 1000000, 10000)
	//启动服务
	job.Start()
	//启动统计脚本
	go jobStats(job)
}

//验证平滑关闭
func termStop(job *work.Job) {
	RegisterWorker2(job)
	//预先生成数据到本地内存队列
	pushQueueData(job, "hts1", 10000)
	pushQueueData(job, "hts2", 10000, 10000)
	pushQueueData(job, "kxy1", 10000, 20000)

	//启动服务
	job.Start()

	//启动统计脚本
	go jobStats(job)

	//结束服务
	time.Sleep(time.Millisecond * 3000)
	job.Stop()
	err := job.WaitStop(time.Second * 10)
	fmt.Println("wait stop return", err);

	//统计数据，查看是否有漏处理的任务
	stat := job.Stats()
	fmt.Println(stat)
	count := len(queues["hts1"]) + len(queues["hts2"]) + len(queues["kxy1"])
	fmt.Println("remain count:", count)
}

func pushQueueData(job *work.Job, topic string, args ...int) {
	ctx := context.Background()
	start := 1
	length := 1
	if len(args) > 0 {
		length = args[0]
		if len(args) > 1 {
			start = args[1]
		}
	}

	strs := make([]string, 0)
	for i := start; i < start+length; i++ {
		strs = append(strs, strconv.Itoa(i))
	}
	if len(args) > 0 {
		job.BatchEnqueue(ctx, topic, strs)
	}
}

func jobStats(job *work.Job) {
	var stat map[string]int64
	var count int64
	var str string


	lastStat := job.Stats()
	keys := make([]string, 0)
	for k, _ := range lastStat {
		keys = append(keys, k)
	}

	for {
		stat = job.Stats()

		str = ""
		for _, k := range keys {
			count = stat[k] - lastStat[k]
			if count > 0 {
				str += k + ":" + strconv.FormatInt(count, 10) + "   "
			}
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), str)

		lastStat = stat
		time.Sleep(time.Second)
	}
}

/**
 * 配置队列任务
 */
func RegisterWorkerBench(job *work.Job) {
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Mock), MaxConcurrency: 100})
}

func Mock(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	return work.TaskResult{Id: task.Id}
}

/**
 * 配置队列任务
 */
func RegisterWorker2(job *work.Job) {
	job.AddFunc("hts1", Me, 5)
	job.AddFunc("hts2", Me, 3)
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Me), MaxConcurrency: 5})
}

func Me(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 50)
	i, _ := strconv.Atoi(task.Message)
	if i%10 == 0 {
		panic("wo cuo le " + task.Message + " (" + task.Topic + ")")
	}
	s, _ := work.JsonEncode(task)
	fmt.Println("do task", s)
	return work.TaskResult{Id: task.Id}
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, message string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], message)
	return true, nil
}

func (q *LocalQueue) BatchEnqueue(ctx context.Context, key string, messages []string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], messages...)
	return true, nil
}

func (q *LocalQueue) Dequeue(ctx context.Context, key string) (message string, token string, err error) {
	lock.Lock()
	defer lock.Unlock()

	if len(queues[key]) > 0 {
		message = queues[key][0]
		queues[key] = queues[key][1:]
	}

	return
}

func (q *LocalQueue) AckMsg(ctx context.Context, key string, token string) (bool, error) {
	return true, nil
}

type MyLogger struct{}

func (logger *MyLogger) Trace(v ...interface{}) {

}

func (logger *MyLogger) Tracef(format string, args ...interface{}) {

}

func (logger *MyLogger) Debug(v ...interface{}) {

}

func (logger *MyLogger) Debugf(format string, args ...interface{}) {

}

func (logger *MyLogger) Info(v ...interface{}) {

}

func (logger *MyLogger) Infof(format string, args ...interface{}) {

}

func (logger *MyLogger) Warn(v ...interface{}) {

}

func (logger *MyLogger) Warnf(format string, args ...interface{}) {

}

func (logger *MyLogger) Error(v ...interface{}) {

}

func (logger *MyLogger) Errorf(format string, args ...interface{}) {

}
