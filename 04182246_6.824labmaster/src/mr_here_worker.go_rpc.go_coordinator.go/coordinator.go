package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	//mapTaskNoAllocated 是一个字段名，其类型为 map[int]bool，即一个映射，其中键是 int 类型，值是 bool 类型。
	mapTaskNoAllocated    map[int]bool
	mapTaskNoDone         map[int]bool
	reduceTaskNoAllocated map[int]bool
	reduceTaskNoDone      map[int]bool
	files                 []string
	num_reducer           int

	mutex sync.Mutex
}

// 在此编写代码--用于工作进程调用的 RPC 处理程序。
// 这是一个示例 RPC 处理程序，RPC 参数和回复类型在 rpc.go 中定义。
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// worker call this function to get task to do
func (c *Coordinator) AcquireTask(args *AcTaskArgs, reply *AcTaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.mapTaskNoAllocated) > 0 {
		for i := range c.mapTaskNoAllocated {
			reply.Task_id = i
			reply.Task_t = MapTask
			reply.Map_file_name = c.files[i]
			reply.Num_reducer = c.num_reducer
			delete(c.mapTaskNoAllocated, i)
			c.mapTaskNoDone[i] = true
			//使用关键字 go可以启动一个新的 goroutine.在下面的示例中，go func() { ... }() 启动了一个新的 goroutine，并在其中运行了一个匿名函数
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				defer c.mutex.Unlock()
				_, ok := c.mapTaskNoDone[task_id]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("map Task", task_id, "time out!")
					delete(c.mapTaskNoDone, task_id)
					c.mapTaskNoAllocated[task_id] = true
				}
			}(i)
			return nil
		}
	} else if len(c.mapTaskNoDone) > 0 {
		// some map task is not done! work should wait
		reply.Task_t = WaitTask
		return nil
	} else if len(c.reduceTaskNoAllocated) > 0 {
		// map task is all done and reduce task is not all done
		for i := range c.reduceTaskNoAllocated {
			reply.Task_id = i
			reply.Task_t = ReduceTask
			reply.Map_task_num = len(c.files)
			delete(c.reduceTaskNoAllocated, i)
			c.reduceTaskNoDone[i] = true
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mutex.Lock()
				defer c.mutex.Unlock()
				_, ok := c.reduceTaskNoDone[task_id]
				if ok {
					// reduce task timeout!
					fmt.Println("reduce task", task_id, "time out!")
					delete(c.reduceTaskNoDone, task_id)
					c.reduceTaskNoAllocated[task_id] = true
				}
			}(i)
			return nil
		}
	} else if len(c.reduceTaskNoDone) > 0 {
		// reduce is not done! so wait
		reply.Task_t = WaitTask
		return nil
	} else {
		// all queue is empty, so the job is done!
		reply.Task_t = WaitTask
		fmt.Println("jog is done!")
		return nil
	}
	return nil
}

// worker call this function to finish task
func (c *Coordinator) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Task_t == MapTask {
		_, ok := c.mapTaskNoDone[args.Task_id]
		if ok {
			// the task is in the NoDone queue
			delete(c.mapTaskNoDone, args.Task_id)
		} else {
			_, ok := c.mapTaskNoAllocated[args.Task_id]
			if ok {
				// the taskid is found in NoAll, this can happen when this is timeout and then send reply
				delete(c.mapTaskNoAllocated, args.Task_id)
			} else {
				// replica reply
				return nil
			}
		}
		// check whether all map task is done and then begin reduce task
		if len(c.mapTaskNoAllocated) == 0 && len(c.mapTaskNoDone) == 0 {
			for i := 0; i < c.num_reducer; i++ {
				c.reduceTaskNoAllocated[i] = true
			}
		}
	} else {
		_, ok := c.reduceTaskNoDone[args.Task_id]
		if ok {
			delete(c.reduceTaskNoDone, args.Task_id)
		} else {
			_, ok := c.reduceTaskNoAllocated[args.Task_id]
			if ok {
				delete(c.reduceTaskNoDone, args.Task_id)
			} else {
				return nil
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go 周期性调用 Done() 函数，以了解整个作业是否已完成
func (c *Coordinator) Done() bool {
	ret := false
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// check whether all queue is empty
	if len(c.mapTaskNoAllocated) == 0 && len(c.mapTaskNoDone) == 0 && len(c.reduceTaskNoAllocated) == 0 && len(c.reduceTaskNoDone) == 0 {
		ret = true
	}
	return ret
}

/*
创建协调器。
main/mrcoordinator.go 调用此函数
nReduce 是要使用的 reduce 任务数
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// init the fields(字段)
	c.mapTaskNoAllocated = make(map[int]bool)
	c.mapTaskNoDone = make(map[int]bool)
	c.reduceTaskNoAllocated = make(map[int]bool)
	c.reduceTaskNoDone = make(map[int]bool)
	c.files = files
	c.num_reducer = nReduce
	for i := 0; i < len(files); i++ {
		c.mapTaskNoAllocated[i] = true
	}

	// Your code here.

	c.server()
	return &c
}
