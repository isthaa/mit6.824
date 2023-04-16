package mr

// RPC definitions.remember to capitalize(大写) all names.

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments and reply for an RPC.

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	WaitTask   TaskType = 2
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AcTaskArgs struct {
}

type AcTaskReply struct {
	Task_t  TaskType // task type
	Task_id int      // task number
	// input file names
	Num_reducer   int
	Map_file_name string
	Map_task_num  int
}

type DoneTaskArgs struct {
	Task_t  TaskType
	Task_id int
}

type DoneTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
/*
这段代码定义了一个名为 coordinatorSock 的函数，其返回值为一个字符串类型的 UNIX 域套接字路径。
该函数将 "/var/tmp/824-mr-" 字符串赋值给变量 s，接着通过 strconv.Itoa(os.Getuid()) 
将当前用户的用户 ID 转换成字符串并拼接到 s 后面，最终返回拼接后的字符串作为 UNIX 域套接字路径。
这段代码的作用是在 /var/tmp 目录下生成一个相对唯一的 UNIX 域套接字名称，以用于协调器（coordinator）
与 MapReduce 任务之间的通信。函数通过将当前用户的用户 ID 添加到固定前缀 "/var/tmp/824-mr-" 中来确保套接字名称的唯一性。
*/
