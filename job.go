package work

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// 默认worker的并发数
	defaultConcurrency = 5
)

const (
	Trace = uint8(iota)
	Debug
	Info
	Warn
	Error
	None
)

var (
	ErrQueueNotExist   = errors.New("queue is not exists")
	ErrTimeout         = errors.New("timeout")
	ErrTopicRegistered = errors.New("the key had been registered")
)

type queueManger struct {
	queue Queue
	// 队列服务负责的主题
	topics []string
}

type Job struct {
	// 上下文
	ctx context.Context

	// workers及map锁
	workers map[string]*Worker
	// map操作锁
	wLock sync.RWMutex

	// worker并发控制通道
	concurrency map[string]chan struct{}
	cLock       sync.RWMutex

	// 队列数据通道
	tasksChan map[string]chan Task
	tLock     sync.RWMutex

	enabledTopics []string

	// work并发处理的等待暂停
	wg sync.WaitGroup
	// 启动状态
	running bool
	// 异常状态时需要sleep时间
	sleepy time.Duration
	// 设置的初始等待时间
	initSleepy time.Duration
	// 设置的等待时间的上限
	maxSleepy time.Duration
	// 通道定时器超时时间
	timer time.Duration
	// 默认的worker并发数
	con int

	// Queue服务 - 依赖外部注入
	queueMangers []queueManger
	// 默认Queue服务 - 依赖外部注入
	defaultQueue Queue
	// topic与queue的映射关系
	queueMap map[string]Queue
	// map操作锁
	qLock sync.RWMutex

	// 标准输出等级
	consoleLevel uint8

	// 日志服务 - 依赖外部注入
	logger Logger
	// 日记等级
	level uint8

	// 是否初始化
	isInit         bool
	isQueueMapInit bool

	// 统计
	pullCount        int64
	pullEmptyCount   int64
	pullErrCount     int64
	taskCount        int64
	taskErrCount     int64
	handleCount      int64
	handleErrCount   int64
	handlePanicCount int64

	// 回调函数
	// 任务返回失败回调函数
	taskErrCallback func(task Task, result TaskResult)
	// 任务panic回调函数 增加参数将panic信息传递给回调函数，方便做sentry处理
	taskPanicCallback func(task Task, e ...interface{})
	// 任务处理前回调
	taskBeforeCallback func(task Task)
	// 任务处理后回调
	taskAfterCallback func(task Task, result TaskResult)
}

// topic是否开启 备注：空的时候默认启用全部
func (j *Job) isTopicEnable(topic string) bool {
	if len(j.enabledTopics) == 0 {
		return true
	}

	for _, t := range j.enabledTopics {
		if t == topic {
			return true
		}
	}
	return false
}

// 初始化workers相关配置
func (j *Job) initWorkers() {
	for topic, w := range j.workers {
		if !j.isTopicEnable(topic) {
			continue
		}

		if w.MaxConcurrency <= 0 {
			w.MaxConcurrency = j.con
		}

		// 用来控制workers的并发数
		j.concurrency[topic] = make(chan struct{}, w.MaxConcurrency)
		for i := 0; i < w.MaxConcurrency; i++ {
			j.concurrency[topic] <- struct{}{}
		}

		// 存放消息数据的通道
		j.tasksChan[topic] = make(chan Task)
	}
}

// 初始化topic与queu的映射关系map
func (j *Job) initQueueMap() {
	j.isQueueMapInit = true
	topicMap := make(map[string]bool)

	for topic := range j.workers {
		topicMap[topic] = true
	}
	j.println(Debug, "topicMap", topicMap)

	for index, qm := range j.queueMangers {
		for _, topic := range qm.topics {
			validTopics := make([]string, 0)
			if _, ok := topicMap[topic]; ok {
				validTopics = append(validTopics, topic)
				delete(topicMap, topic)
			}
			j.println(Debug, "validTopics", validTopics, index)
			if len(validTopics) > 0 {
				for _, topic := range validTopics {
					j.setQueueMap(qm.queue, topic)
				}
			}
		}
	}

	if j.defaultQueue == nil {
		return
	}

	remainTopics := make([]string, 0)
	for topic, ok := range topicMap {
		if ok {
			remainTopics = append(remainTopics, topic)
		}
	}
	j.println(Debug, "remainTopics", remainTopics)
	if len(remainTopics) > 0 {
		for _, topic := range remainTopics {
			j.setQueueMap(j.defaultQueue, topic)
		}
	}
}

// 启动拉取队列数据服务
func (j *Job) runQueues() {
	for topic, queue := range j.queueMap {
		if !j.isTopicEnable(topic) {
			continue
		}
		go j.watchQueueTopic(queue, topic)
	}
}

// 监听队列某个topic
func (j *Job) watchQueueTopic(q Queue, topic string) {
	j.println(Info, "watch queue topic", topic)
	j.cLock.RLock()
	conChan := j.concurrency[topic]
	j.cLock.RUnlock()

	timer := time.NewTimer(j.timer)
	for {
		if !j.running {
			j.println(Info, "stop watch queue topic", topic)
			return
		}

		select {
		case <-conChan:
			go j.pullTask(q, topic)
		case <-timer.C:
			timer.Reset(j.timer)
			continue
		}
	}
}

// topic与queue的map映射关系表，主要是ack通过Topic获取
func (j *Job) setQueueMap(q Queue, topic string) {
	j.qLock.Lock()
	j.queueMap[topic] = q
	j.qLock.Unlock()
}

// 拉取队列消息
func (j *Job) pullTask(q Queue, topic string) {
	var taskEnqueue bool

	j.wg.Add(1)
	defer func() {
		j.wg.Done()

		//任务没有入队时需要给缓存通道重新筛数据，保证上游拉取队列消息的持续运行
		if !taskEnqueue {
			j.concurrency[topic] <- struct{}{}
		}
	}()

	j.wLock.RLock()
	work := j.workers[topic]
	j.wLock.RUnlock()
	extraParams := work.ExtraParam
	realTopic := topic
	// todo 临时方案：将extraParams第4个参数，强制指定为topic，来解除耦合
	if len(extraParams) > 3 {
		tempTopic, ok := extraParams[3].(string)
		if ok {
			realTopic = tempTopic
		}
	}
	message, tag, token, dequeueCount, err := q.Dequeue(j.ctx, realTopic, extraParams...)
	atomic.AddInt64(&j.pullCount, 1)
	if err != nil && err != ErrNil {
		atomic.AddInt64(&j.pullErrCount, 1)
		j.logAndPrintln(Error, "dequeue_error", err, message)
		j.JobSleep()
		return
	}

	//无消息时，sleep
	if err == ErrNil || message == "" {
		j.println(Trace, "return nil message", topic)
		atomic.AddInt64(&j.pullEmptyCount, 1)
		j.JobSleep()
		return
	}
	atomic.AddInt64(&j.taskCount, 1)

	// todo 临时方案：将extraParams第5个参数，强制指定是否使用原生消息。默认情况，保持原先逻辑，使用snow框架自带的消息结构
	flag := false
	task := Task{}
	if len(extraParams) > 4 {
		useOriMsgFlag, ok := extraParams[4].(bool)
		if ok {
			flag = useOriMsgFlag
		}
	}
	if flag {
		task.Message = message
		task.Id = token
	} else {
		task, err = DecodeStringTask(message)
		if err != nil {
			atomic.AddInt64(&j.taskErrCount, 1)
			j.logAndPrintln(Error, "decode_task_error", err, message)
			j.JobSleep()
			return
		}
	}

	// token为必须参数,用于后续ack
	task.Token = token
	task.Tag = tag

	if j.sleepy != j.initSleepy {
		j.ResetJobSleep()
	}
	task.DequeueCount = dequeueCount

	j.tLock.RLock()
	tc := j.tasksChan[topic]
	j.tLock.RUnlock()

	timer := time.NewTimer(j.timer)
	for {
		select {
		case tc <- task:
			taskEnqueue = true
			j.println(Debug, "taskChan push after", task, time.Now())
			return
		case <-timer.C:
			timer.Reset(j.timer)
			continue
		}
	}
}

func (j *Job) processJob() {
	for topic, taskChan := range j.tasksChan {
		go j.processWork(topic, taskChan)
	}
}

// 读取通道数据分发到各个topic对应的worker进行处理
func (j *Job) processWork(topic string, taskChan <-chan Task) {
	defer func() {
		if e := recover(); e != nil {
			j.logAndPrintln(Error, "process_task_panic", e)
		}
	}()

	timer := time.NewTimer(j.timer)
	for {
		select {
		case task := <-taskChan:
			go j.processTask(topic, task)
		case <-timer.C:
			timer.Reset(j.timer)
			continue
		}
	}
}

// 处理task任务
func (j *Job) processTask(topic string, task Task) TaskResult {
	j.wg.Add(1)
	defer func() {
		j.wg.Done()
		j.concurrency[topic] <- struct{}{}

		//任务panic回调函数
		if e := recover(); e != nil {
			atomic.AddInt64(&j.handlePanicCount, 1)
			if j.taskPanicCallback != nil {
				j.taskPanicCallback(task, e)
			} else {
				j.logAndPrintln(Error, "task_panic", task, e)
			}
		}
	}()

	j.wLock.RLock()
	w := j.workers[topic]
	j.wLock.RUnlock()

	//任务处理前回调函数
	if j.taskBeforeCallback != nil {
		j.taskBeforeCallback(task)
	}

	result := w.Call.Run(task)

	//多线程安全加减
	atomic.AddInt64(&j.handleCount, 1)

	var (
		isAck bool
	)
	switch result.State {
	case StateSucceed:
		isAck = true
	case StateFailedWithRetryNumLimit:
		isAck = true
	case StateFailedWithAck:
		isAck = true
		atomic.AddInt64(&j.handleErrCount, 1)
	case StateFailed:
		atomic.AddInt64(&j.handleErrCount, 1)
	}

	//消息ACK
	if isAck && task.Token != "" {
		extraParams := w.ExtraParam
		realTopic := topic
		// todo 临时方案：将extraParams第4个参数，强制指定为topic，来解除耦合
		if len(extraParams) > 3 {
			tempTopic, ok := extraParams[3].(string)
			if ok {
				realTopic = tempTopic
			}
		}
		_, err := j.GetQueueByTopic(topic).AckMsg(j.ctx, realTopic, task.Token, extraParams...)
		if err != nil {
			j.logAndPrintln(Error, "ack_error", topic, task)
		}
	}

	//任务处理后回调函数
	if j.taskAfterCallback != nil {
		j.taskAfterCallback(task, result)
	}

	return result
}

// After there is no data, the job starts from initsleepy to sleep,
// and then multiplies to maxsleepy. After finding the data, it sleep from initsleepy again
func (j *Job) JobSleep() {
	if j.sleepy.Nanoseconds()*2 < j.maxSleepy.Nanoseconds() {
		j.sleepy = time.Duration(j.sleepy.Nanoseconds() * 2)
	} else if j.sleepy.Nanoseconds()*2 >= j.maxSleepy.Nanoseconds() && j.sleepy != j.maxSleepy {
		if j.sleepy < j.maxSleepy {
			j.sleepy = j.maxSleepy
		}
	}
	time.Sleep(j.sleepy)
}

func (j *Job) ResetJobSleep() {
	j.SetSleepy(j.initSleepy)
}
