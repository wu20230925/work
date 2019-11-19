package work

type Worker struct {
	Call           WorkerFunc
	MaxConcurrency int
	ExtraParam     []interface{} // 为了一些特殊驱动需要参数
}

type WorkerFunc interface {
	Run(task Task) TaskResult
}

type MyWorkerFunc func(task Task) TaskResult

func (f MyWorkerFunc) Run(task Task) TaskResult {
	return f(task)
}
