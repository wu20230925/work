package work

import (
	"encoding/json"
)

const (
	//成功，默认会触发ack
	StateSucceed = iota
	//失败，默认不会触发ack 说明：没有触发ack，如果queue服务支持，会进行消息重放
	StateFailed
	//失败，会触发ack
	StateFailedWithAck
	//失败，出队次数超过限制 也会触发ack
	StateFailedWithRetryNumLimit
)

type Task struct {
	Id           string `json:"id"`
	Topic        string `json:"topic"`
	Message      string `json:"message"`
	Tag          string `json:"tag"`
	Token        string
	DequeueCount int64
}

func (t Task) String() string {
	return string(t.Bytes())
}

func (t Task) Bytes() []byte {
	bytes, _ := json.Marshal(t)
	return bytes
}

func DecodeStringTask(s string) (t Task, err error) {
	t, err = DecodeBytesTask([]byte(s))
	return
}

func DecodeBytesTask(b []byte) (t Task, err error) {
	err = json.Unmarshal(b, &t)
	return
}

type TaskResult struct {
	Id      string
	State   int
	Message string
}

func GenTask(topic string, message string) Task {
	return Task{Id: GenUUID(), Topic: topic, Message: message}
}
