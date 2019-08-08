package work

import (
	"time"
	"context"
)

func New() *Job {
	j := new(Job)
	j.ctx = context.Background()
	j.workers = make(map[string]*Worker)
	j.concurrency = make(map[string]chan struct{})
	j.tasksChan = make(map[string]chan Task)
	j.queueMap = make(map[string]Queue)
	j.level = Info
	j.consoleLevel = Info
	j.sleepy = time.Millisecond * 10
	j.timer = time.Millisecond * 30
	j.con = defaultConcurrency
	return j
}

func (j *Job) Start() {
	if j.running {
		return
	}

	j.isInit = true
	j.running = true
	j.initWorkers()
	j.initQueueMap()
	j.runQueues()
	j.processJob()
}

/**
 * 暂停Job
 */
func (j *Job) Stop() {
	if !j.running {
		return
	}
	j.running = false
}

/**
 * 等待队列任务消费完成，可设置超时时间返回
 * @param timeout 如果小于0则默认10秒
 */
func (j *Job) WaitStop(timeout time.Duration) error {
	ch := make(chan struct{})

	time.Sleep(j.sleepy * 2)
	if timeout <= 0 {
		timeout = time.Second * 10
	}

	go func() {
		j.wg.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	}

	return nil
}

func (j *Job) AddFunc(topic string, f func(task Task) (TaskResult), args ...interface{}) error {
	//worker并发数
	var concurrency int
	if len(args) > 0 {
		if c, ok := args[0].(int); ok {
			concurrency = c
		}
	}
	w := &Worker{Call: MyWorkerFunc(f), MaxConcurrency: concurrency}
	return j.AddWorker(topic, w)
}

func (j *Job) AddWorker(topic string, w *Worker) error {
	j.wLock.Lock()
	defer j.wLock.Unlock()

	if _, ok := j.workers[topic]; ok {
		return ErrTopicRegistered
	}

	j.workers[topic] = w

	j.printf(Info, "topic(%s) concurrency %d\n", topic, w.MaxConcurrency)
	return nil
}

//获取统计数据
func (j *Job) Stats() map[string]int64 {
	return map[string]int64{
		"pull":         j.pullCount,
		"pull_err":     j.pullErrCount,
		"pull_empty":   j.pullEmptyCount,
		"task":         j.taskCount,
		"task_err":     j.taskErrCount,
		"handle":       j.handleCount,
		"handle_err":   j.handleErrCount,
		"handle_panic": j.handlePanicCount,
	}
}

//设置worker默认并发数
func (j *Job) SetConcurrency(concurrency int) {
	if concurrency <= 0 {
		return
	}
	j.con = concurrency
}

//设置休眠的时间 -- 碰到异常或者空消息等情况
func (j *Job) SetSleepy(sleepy time.Duration) {
	j.sleepy = sleepy
}

//在通道传递数据时的阻塞超时时间
func (j *Job) SetTimer(timer time.Duration) {
	j.timer = timer
}

//设置标准输出日志等级
func (j *Job) SetConsoleLevel(level uint8) {
	j.consoleLevel = level
}

//设置文件输出日志等级
func (j *Job) SetLevel(level uint8) {
	j.level = level
}

//设置日志服务
func (j *Job) SetLogger(logger Logger) {
	j.logger = logger
}

//针对性开启topics
func (j *Job) SetEnableTopics(topics ...string) {
	j.enabledTopics = topics
}

//设置任务处理前回调函数
func (j *Job) RegisterTaskBeforeCallback(f func(task Task)) {
	j.taskBeforeCallback = f
}

//设置任务处理后回调函数
func (j *Job) RegisterTaskAfterCallback(f func(task Task, taskResult TaskResult)) {
	j.taskAfterCallback = f
}

//设置任务panic回调函数：回调函数自己确保panic不会上报，否则会导致此topic的队列worker停止
func (j *Job) RegisterTaskPanicCallback(f func(task Task, e ...interface{})) {
	j.taskPanicCallback = f
}
