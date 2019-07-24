## Queue worker
基于Go语言实现的队列调度服务

## Features
- 非侵入式的Queue接口设计，解耦队列调度服务与Queue驱动，Queue驱动自行扩展实现；
- 非侵入式的Logger接口设计，Logger自行扩展实现，通过SetLogger注入；
- 简洁易用的Worker注册机制，开发只需要关心业务逻辑的实现；
- 支持日志等级、标准输出等级控制；
- 支持worker任务事件注册回调:处理前、处理后、panic；
- 支持针对性开启topic的消费worker；
- 支持平滑关闭和超时退出机制；
- 支持简单的消息入队调用；

## Get started

### New service
```
j = job.New()
```

### Register worker
```
//设置worker的任务投递回调函数
j.AddFunc("topic:test", test)
//设置worker的任务投递回调函数，和并发数
j.AddFunc("topic:test1", test, 2)
//使用worker结构进行注册
j.AddWorker("topic:test2", &work.Worker{Call: work.MyWorkerFunc(test), MaxConcurrency: 1})
```

### Register event
```
//任务处理前的回调函数
j.RegisterTaskBeforeCallback(task Task)
//任务处理后的回调函数
j.RegisterTaskAfterCallback(task Task, taskResult TaskResult)
//任务处理触发panic的回调函数
j.RegisterTaskBeforeCallback(task Task)
```

### Register queue driver
```
//针对topic设置相关的queue,需要实现work.Queue接口的方法
job.AddQueue(&LocalQueue{}, "topic1", "topic2")

//设置默认的queue, 没有设置过的topic会使用默认的queue
job.AddQueue(&LocalQueue{})
```

### Setter
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

### How to start
```
j.Start()
```

### How to stop
停止服务只会影响队列消费，不会影响消息入队的调用
```
//将服务设置为停止状态
//j.Stop()

//waitStop会等待worker任务跑完后停止当前服务。
//第一个参数为超时时间，如果无法获取到worker全部停止状态，在超时时间后会return一个超时错误
//job.WaitStop(time.Second * 3)
```

### Get stats
```
//获取运行态统计数据
j.Stats()
```

### Enqueue
```
//消息入队
job.Enqueue(ctx context.Context, topic string, message string, args ...interface{})
//消息入队以Task数据结构
job.EnqueueWithTask(ctx context.Context, topic string, task work.Task, args ...interface{})
//消息批量入队
job.BatchEnqueue(ctx context.Context, topic string, messages []string, args ...interface{})
//消息批量入队以Task数据结构
job.BatchEnqueueWithTask(ctx context.Context, topic string, tasks []work.Task, args ...interface{})
```

## Bench
### Condition
设置worker并发度100，worker模拟耗时0.005ms，本地队列100W数据。

### Run
```golang
go run example/example.go
```

### Result
稳定在19000tps左右（100并发+耗时0.005ms的极限是20000tps），损耗几乎集中在内存队列操作的耗时(本地内存队列没好好实现)，本身实现的损耗非常小。

### Picture of result
pull表示从Queue驱动拉取的消息数，task表示任务分发数，handle表示任务处理数  

<img src='docs/bench1.png' width="300">

### How to calculate speed
1s/(队列驱动单次拉取数据耗时+worker单任务处理耗时) * worker并发数

## More

### A Demo
```text
example/job.go
```
