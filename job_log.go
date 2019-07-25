package work

import "fmt"

//是否达到标准输出等级
func (j *Job) reachConsoleLevel(level uint8) bool {
	return level >= j.consoleLevel
}

//标准输出
func (j *Job) println(level uint8, a ...interface{}) {
	if !j.reachConsoleLevel(level) {
		return
	}
	fmt.Println(a...)
}

//格式化标准输出
func (j *Job) printf(level uint8, format string, a ...interface{}) {
	if !j.reachConsoleLevel(level) {
		return
	}
	fmt.Printf(format, a...)
}

//是否达到输出日志等级
func (j *Job) reachLevel(level uint8) bool {
	return level >= j.level
}

//打印日志
func (j *Job) log(level uint8, a ...interface{}) {
	if j.logger == nil {
		return
	}
	if !j.reachLevel(level) {
		return
	}
	switch level {
	case Trace:
		j.logger.Trace(a...)
	case Debug:
		j.logger.Debug(a...)
	case Info:
		j.logger.Info(a...)
	case Warn:
		j.logger.Warn(a...)
	case Error:
		j.logger.Error(a...)
	}
}

//格式化打印日志
func (j *Job) logf(level uint8, format string, a ...interface{}) {
	if j.logger == nil {
		return
	}
	if !j.reachLevel(level) {
		return
	}
	switch level {
	case Trace:
		j.logger.Tracef(format, a...)
	case Debug:
		j.logger.Debugf(format, a...)
	case Info:
		j.logger.Infof(format, a...)
	case Warn:
		j.logger.Warnf(format, a...)
	case Error:
		j.logger.Errorf(format, a...)
	}
}

//日志和标准输出
func (j *Job) logAndPrintln(level uint8, a ...interface{}) {
	j.log(level, a...)
	j.println(level, a...)
}

func (j *Job) logfAndPrintf(level uint8, format string, a ...interface{}) {
	j.logf(level, format, a...)
	j.printf(level, format, a...)
}
