package work

import "context"

//获取topic对应的queue服务
func (j *Job) GetQueueByTopic(topic string) Queue {
	j.qLock.RLock()
	q, ok := j.queueMap[topic]
	j.qLock.RUnlock()
	if !ok {
		return nil
	}
	return q
}

/**
 * 往Job注入Queue服务
 */
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


//消息入队 -- 原始message
func (j *Job) Enqueue(ctx context.Context, topic string, message string, args ...interface{}) (bool, error) {
	task := GenTask(topic, message)
	return j.EnqueueWithTask(ctx, topic, task, args...)
}

//消息入队 -- Task数据结构
func (j *Job) EnqueueWithTask(ctx context.Context, topic string, task Task, args ...interface{}) (bool, error) {
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

//消息入队 -- 原始message
func (j *Job) BatchEnqueue(ctx context.Context, topic string, messages []string, args ...interface{}) (bool, error) {
	tasks := make([]Task, len(messages))
	for k, message := range messages {
		tasks[k] = GenTask(topic, message)
	}
	return j.BatchEnqueueWithTask(ctx, topic, tasks, args...)
}

//消息入队 -- Task数据结构
func (j *Job) BatchEnqueueWithTask(ctx context.Context, topic string, tasks []Task, args ...interface{}) (bool, error) {
	if !j.isQueueMapInit {
		j.initQueueMap()
	}
	q := j.GetQueueByTopic(topic)
	if q == nil {
		return false, ErrQueueNotExist
	}

	arr := make([]string, len(tasks))
	for k, task := range tasks {
		if task.Topic == "" {
			task.Topic = topic
		}
		s, _ := JsonEncode(task)
		arr[k] = s
	}
	return q.BatchEnqueue(ctx, topic, arr, args...)
}