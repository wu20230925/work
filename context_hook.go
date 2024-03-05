package work

import (
	"context"
	"fmt"
	"time"

	"github.com/SkyAPM/go2sky"
	v3 "github.com/SkyAPM/go2sky/reporter/grpc/language-agent"
)

// ContextHook represents a hook context
type ContextHook struct {
	start       time.Time
	Ctx         context.Context
	Topic       string // topic
	Args        []interface{}
	ExecuteTime time.Duration
	Err         error // execute error
	span        go2sky.Span
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

// End finish the hook invokation
func (c *ContextHook) End(ctx context.Context, err error) {
	c.Ctx = ctx
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
		var err error
		if err = hk.BeforeProcess(c); err != nil {
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

type HookImpl struct {
	tracer *go2sky.Tracer
}

type Go2skyKey interface{}

func NewHook(tracer *go2sky.Tracer) Hook {
	return &HookImpl{tracer: tracer}
}

func (h *HookImpl) BeforeProcess(c *ContextHook) error {
	peer := "queue"
	if p, ok := c.Ctx.Value("peer").(string); ok {
		peer = p
	}

	args := fmt.Sprintf("%v", c.Args)
	span, nCtx, err := h.tracer.CreateLocalSpan(c.Ctx, go2sky.WithSpanType(go2sky.SpanTypeLocal), go2sky.WithOperationName(fmt.Sprintf("enqueue_%v", c.Topic)))
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	span.SetPeer(peer)
	span.Tag("args", args)
	span.Tag("topic", c.Topic)
	span.SetSpanLayer(v3.SpanLayer_MQ)
	c.span = span
	c.Ctx = nCtx
	return nil
}

func (h *HookImpl) AfterProcess(c *ContextHook) error {
	span := c.span
	if c.ExecuteTime > 0 {
		span.Tag("elapsed", fmt.Sprintf("%d ms", c.ExecuteTime.Milliseconds()))
	}
	if c.Err != nil {
		timeNow := time.Now()
		span.Error(timeNow, c.Err.Error())
	}
	span.End()
	return nil
}
