package work

import (
	"context"
)

// GetQueueByTopic 获取topic对应的queue服务
func (j *Job) GetQueueByTopic(topic string) Queue {
	j.qLock.RLock()
	q, ok := j.queueMap[topic]
	j.qLock.RUnlock()
	if !ok {
		return nil
	}
	return q
}

// AddQueue 往Job注入Queue服务
func (j *Job) AddQueue(q Queue, topics ...string) {
	if len(topics) > 0 {
		qm := queueManger{
			queue:  q,
			topics: topics,
		}
		j.queueMangers = append(j.queueMangers, qm)
	} else {
		j.defaultQueue = q
	}
}

// Enqueue 消息入队 -- 原始message
func (j *Job) Enqueue(ctx context.Context, topic string, message string, args ...interface{}) (bool, error) {
	task := GenTask(topic, message)
	return j.EnqueueWithTask(ctx, topic, task, args...)
}

// EnqueueWithTask 消息入队 -- Task数据结构
func (j *Job) EnqueueWithTask(ctx context.Context, topic string, task Task, args ...interface{}) (bool, error) {
	hookCtx := NewContextHook(ctx, topic, args)
	err := j.BeforeProcess(hookCtx)
	if err != nil {
		return false, err
	}
	success, err := j.enqueueWithTask(hookCtx.Ctx, topic, task, args)
	hookCtx.End(hookCtx.Ctx, err)
	err = j.AfterProcess(hookCtx)

	return success, err
}

func (j *Job) enqueueWithTask(ctx context.Context, topic string, task Task, args []interface{}) (bool, error) {
	if !j.isQueueMapInit {
		j.initQueueMap()
	}
	q := j.GetQueueByTopic(topic)
	if q == nil {
		return false, ErrQueueNotExist
	}

	if task.Topic == "" {
		task.Topic = topic
	}
	s, _ := JsonEncode(task)
	return q.Enqueue(ctx, topic, s, args...)
}

// EnqueueRaw 消息入队 -- 原始message不带有task结构原生消息
func (j *Job) EnqueueRaw(ctx context.Context, topic string, message string, args ...interface{}) (bool, error) {
	hookCtx := NewContextHook(ctx, topic, args)
	err := j.BeforeProcess(hookCtx)
	if err != nil {
		return false, err
	}
	success, err := j.enqueueRaw(hookCtx.Ctx, topic, message, args)
	hookCtx.End(hookCtx.Ctx, err)
	err = j.AfterProcess(hookCtx)

	return success, err
}

func (j *Job) enqueueRaw(ctx context.Context, topic string, message string, args []interface{}) (bool, error) {
	if !j.isQueueMapInit {
		j.initQueueMap()
	}
	q := j.GetQueueByTopic(topic)
	if q == nil {
		return false, ErrQueueNotExist
	}
	return q.Enqueue(ctx, topic, message, args...)
}

// BatchEnqueue 消息入队 -- 原始message
func (j *Job) BatchEnqueue(ctx context.Context, topic string, messages []string, args ...interface{}) (bool, error) {
	tasks := make([]Task, len(messages))
	for k, message := range messages {
		tasks[k] = GenTask(topic, message)
	}
	return j.BatchEnqueueWithTask(ctx, topic, tasks, args...)
}

// BatchEnqueueWithTask 消息入队 -- Task数据结构
func (j *Job) BatchEnqueueWithTask(ctx context.Context, topic string, tasks []Task, args ...interface{}) (bool, error) {
	messages := make([]string, len(tasks))
	for k, task := range tasks {
		if task.Topic == "" {
			task.Topic = topic
		}
		s, _ := JsonEncode(task)
		messages[k] = s
	}

	hookCtx := NewContextHook(ctx, topic, args)
	err := j.BeforeProcess(hookCtx)
	if err != nil {
		return false, err
	}
	success, err := j.batchEnqueueWithTask(hookCtx.Ctx, topic, messages, args)
	hookCtx.End(hookCtx.Ctx, err)
	err = j.AfterProcess(hookCtx)

	return success, err
}

func (j *Job) batchEnqueueWithTask(ctx context.Context, topic string, messages []string, args []interface{}) (bool, error) {
	if !j.isQueueMapInit {
		j.initQueueMap()
	}
	q := j.GetQueueByTopic(topic)
	if q == nil {
		return false, ErrQueueNotExist
	}
	return q.BatchEnqueue(ctx, topic, messages, args...)
}
