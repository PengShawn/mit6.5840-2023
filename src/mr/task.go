package mr

import "time"

const (
	HeartDuration = 2 * time.Second
	WorkerTimeOut = 10 * time.Second
	WaitingTime   = 5 * time.Second
)

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     TaskState // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

type Task struct {
	files    []string
	id       int64
	taskType TaskType // 任务类型判断到底是map还是reduce
	nReduce  int      // 传入的reducer的数量，用于hash
}

// TaskArgs rpc应该传入的参数，可实际上应该什么都不用传,因为只是worker获取一个任务
type TaskArgs struct{}

type SchedulePhase int
type TaskType int
type TaskState int

//枚举调度器阶段的类型
const (
	MapPhase    SchedulePhase = iota // 此阶段在分发MapTask
	ReducePhase                      // 此阶段在分发ReduceTask
	AllDone                          // 此阶段已完成
)

// 枚举任务阶段的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask // Waiting任务代表此时任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask
)

// 枚举任务状态类型
const (
	Waiting TaskState = iota // 此阶段在等待执行
	Working                  // 此阶段在工作
	Done                     // 此阶段已经做完
)
