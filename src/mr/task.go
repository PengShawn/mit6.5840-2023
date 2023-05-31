package mr

import "time"

const (
	HeartDuration = 1 * time.Second
	WorkerTimeOut = 10 * time.Second
)

type Server struct {
	status          ServerStatus
	taskType        TaskType  // 运行的任务类型
	mapTaskDone     []string  //已经完成的map任务
	mapTaskDoing    []string  //正在执行的map任务
	reduceTaskDone  []int     //已经完成的reduce任务
	reduceTaskDoing []int     //正在执行的reduce任务
	lastHeartBeat   time.Time //上一次的心跳时期
}

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	taskType  TaskType
}

type SchedulePhase int
type TaskType int
type ServerStatus int

//枚举调度器阶段的类型
const (
	MapPhase SchedulePhase = iota
	ReducePhase
	AllDone
)

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

const (
	Waiting ServerStatus = iota
	Working
	Done
)
