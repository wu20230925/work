package work

import (
	"context"
	"time"
)

// ContextHook represents a hook context
type ContextHook struct {
	start       time.Time
	Ctx         context.Context
	Topic       string // topic
	Args        []interface{}
	ExecuteTime time.Duration
	Err         error // execute error
}

// NewContextHook return context for hook
func NewContextHook(ctx context.Context, topic string, args []interface{}) *ContextHook {
	return &ContextHook{
		start: time.Now(),
		Ctx:   ctx,
		Topic: topic,
		Args:  args,
	}
}

// End finish the hook invoke
func (c *ContextHook) End(err error) {
	c.Err = err
	c.ExecuteTime = time.Since(c.start)
}

// Hook represents a hook behaviour
type Hook interface {
	BeforeProcess(c *ContextHook) error
	AfterProcess(c *ContextHook) error
}

// AddHook adds a Hook
func (j *Job) AddHook(hooks ...Hook) {
	j.pdHooks = append(j.pdHooks, hooks...)
}

// BeforeProcess invoked before execute the process
func (j *Job) BeforeProcess(c *ContextHook) error {
	for _, hk := range j.pdHooks {
		if err := hk.BeforeProcess(c); err != nil {
			return err
		}
	}
	return nil
}

// AfterProcess invoked after execute the process
func (j *Job) AfterProcess(c *ContextHook) error {
	firstErr := c.Err
	for _, hk := range j.pdHooks {
		err := hk.AfterProcess(c)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
