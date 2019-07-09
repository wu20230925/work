## Queue worker
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

## Quick start
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

## Bench
条件：设置worker并发度100，worker模拟耗时0.005ms，本地队列100W数据。  
结果：稳定在18500-19000tps（100并发+耗时0.005ms的极限是20000tps），几乎无损耗   
测试代码：example/example.go

## More
```
example/job.go 是一个详细的示例文件
example/example.go 是一个可以跑起来的测试案例，可以通过go run example/example.go运行
```