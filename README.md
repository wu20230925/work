## Queue Worker
基于Go语言实现的队列调度服务

## Features
- 非侵入式的Queue接口设计，解耦队列调度服务与Queue驱动，Queue驱动自行扩展实现；
- 非侵入式的Logger接口设计，Logger自行扩展实现，通过SetLogger注入；
- 简洁易用的Worker注册机制，开发只需要关心业务逻辑的实现；
- 支持日志等级、标准输出等级控制；
- 支持worker任务错误返回回调注册；
- 支持worker任务panic回调函数注册；
- 支持针对性开启topic的消费worker；
- 支持平滑关闭+超时保护机制；

## Quick Start
```golang

function main(){
    //实例化一个队列调度服务
    job := work.New()
    //注册worker
    RegisterWorker(job)
    //设置queue驱动
    RegisterQueueDriver(job)
    //设置参数
	SetOptions(job)
    //启动服务
    job.Start()
}

/**
 * 注册任务worker
 */
func RegisterWorker(job *work.Job) {
	//设置worker的任务投递回调函数
	job.AddFunc("topic:test", test)
	//设置worker的任务投递回调函数，和并发数
	job.AddFunc("topic:test1", test, 2)
	//使用worker结构进行注册
	job.AddWorker("topic:test2", &work.Worker{Call: work.MyWorkerFunc(test), MaxConcurrency: 1})
}

/**
 * 给topic注册对应的队列服务
 */
func RegisterQueueDriver(job *work.Job) {
	//针对topic设置相关的queue,需要实现work.Queue接口的方法
	job.AddQueue(queue1, "topic:test1", "topic:test2")
	//设置默认的queue, 没有设置过的topic会使用默认的queue
	job.AddQueue(queue2)
}

/**
 * 任务投递回调函数
 * 备注：任务处理逻辑不要异步化，否则无法控制worker并发数，需要异步化的需要阻塞等待异步化结束，如wg.Wait()
 */
func test(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	s, err := work.JsonEncode(task)
	if err != nil {
		//work.StateFailed 不会进行ack确认
		//work.StateFailedWithAck 会进行ack确认
		//return work.TaskResult{Id: task.Id, State: work.StateFailed}
		return work.TaskResult{Id: task.Id, State: work.StateFailedWithAck}
	} else {
        //work.StateSucceed 会进行ack确认
		fmt.Println("do task", s)
		return work.TaskResult{Id: task.Id, State: work.StateSucceed}
	}

}
```

## Get Started

### New Job Worker Service
```
j = job.New()
```

### Event Register
```
//任务处理前的回调函数
j.RegisterTaskBeforeCallback(task Task)
//任务处理后的回调函数
j.RegisterTaskAfterCallback(task Task, result Task)
//任务处理触发panic的回调函数
j.RegisterTaskBeforeCallback(task Task)
```

### Register Queue Driver
```
//针对topic设置相关的queue,需要实现work.Queue接口的方法
job.AddQueue(&LocalQueue{}, "topic1", "topic2")

//设置默认的queue, 没有设置过的topic会使用默认的queue
job.AddQueue(&LocalQueue{})
```

### Set Control
```
//设置logger，需要实现work.Logger接口的方法
j.SetLogger(&MyLogger{})

//设置logger日志等级，默认work.Info
j.SetLevel(work.Warn)

//设置console输出等级,默认work.Warn
j.SetConsoleLevel(work.Warn)

//设置worker默认的并发数，默认为5
j.SetConcurrency(10)

//设置启用的topic，未设置表示启用全部注册过topic
j.SetEnableTopics("topic1", "topic2")
```

### How to Start
```
j.Start()
```

### How to Stop
```
//将服务设置为停止状态
//j.Stop()

//waitStop会等待worker任务跑完后停止当前服务。
//第一个参数为超时时间，如果无法获取到worker全部停止状态，在超时时间后会return一个超时错误
//job.WaitStop(time.Second * 3)
```

### Stats
```
//获取运行态统计数据
j.Stats()
```

## Bench
条件：设置worker并发度100，worker模拟耗时0.005ms，本地队列100W数据。

结果：稳定在19000tps左右（100并发+耗时0.005ms的极限是20000tps），损耗几乎集中在内存队列操作的耗时(本地内存队列没好好实现)，本身实现的损耗非常小。

测试代码：go run example/example.go

测试截图：  
pull表示从Queue驱动拉取的消息数，task表示任务分发数，handle表示任务处理数  

<img src='docs/bench1.png' width="300">


## More
```
# 一个详细的示例文件
example/job.go

# 单topic消费速度近似计算公式
1s/(队列驱动单次拉取数据耗时+worker单任务处理耗时) * worker并发数
```