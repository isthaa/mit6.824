package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted(发出) by Map.
//

/*
这段代码定义了一个函数 ihash，它的作用是计算字符串类型的 key 的哈希值，并返回一个整数类型的结果。
函数内部使用了 Go 标准库中的 fnv 包，其中的 New32a 函数创建了一个 32 位的 FNV-1a 哈希算法对象。
然后将 key 转换为字节数组，使用 h.Write 方法将其写入哈希算法对象中，最后通过 h.Sum32 方法获取
计算后的哈希值，并使用位运算将其转换为非负整数。
具体来说，函数返回值是哈希值的低 31 位（因为右侧的 & 0x7fffffff 操作会将最高位的符号位设置为 0）。
该哈希值可用于在哈希表等数据结构中进行查找和插入操作。
*/
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
/*
这段代码实现了一个 MapReduce 模型中的 Worker 节点。Worker 通过向 Coordinator 发送请求，获取需要处理的任务。
任务分为 MapTask、ReduceTask 和 WaitTask 三种类型，Worker 根据任务类型进行不同的处理。
如果任务类型为 MapTask，则打开对应的文件并读取文件内容，调用用户提供的 Map 函数进行处理，并根据 Reduce 数量将处理结果按照键值分组。
将分组后的结果写入对应的文件中，并告知 Coordinator 任务已完成。如果任务类型为 ReduceTask，则读取所有 Map 任务输出的文件，
将键值相同的记录聚合起来，然后调用用户提供的 Reduce 函数进行处理，将处理结果写入对应的输出文件中，并告知 Coordinator 任务已完成。
如果任务类型为 WaitTask，则 Worker 休眠一秒钟后重新发送请求。如果任务类型无法识别，则输出错误信息并退出程序。
*/
func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string) {

	// Your worker implementation here.

	for {//表示无限循环
		args := AcTaskArgs{}//创建了一个新的 AcTaskArgs 结构体变量，并将其初始化为空值。
		reply := AcTaskReply{}
		ok := call("Coordinator.AcquireTask", &args, &reply)//call返回值类型是bool型，ok是一个bool型变量
		if !ok {
			log.Fatal("call rpc error! work exist!")
		}
		if reply.Task_t == MapTask {
			filename := reply.Map_file_name
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			tmp_cont := make([][]KeyValue, reply.Num_reducer)
			for i := 0; i < reply.Num_reducer; i++ {
				tmp_cont[i] = []KeyValue{}
			}
			for _, kv := range kva {
				idx := ihash(kv.Key) % reply.Num_reducer
				tmp_cont[idx] = append(tmp_cont[idx], kv)
			}
			for i := 0; i < reply.Num_reducer; i++ {
				out_file_name := fmt.Sprintf("mr-%d-%d", reply.Task_id, i)
				ofile, err := ioutil.TempFile("", out_file_name)
				if err != nil {
					log.Fatal(err)
				}

				enc := json.NewEncoder(ofile)
				for _, kv := range tmp_cont[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				ofile.Close()
				// rename file atomic
				os.Rename(ofile.Name(), out_file_name)
			}
			go func(task_id int) {
				Done_args := DoneTaskArgs{}
				Done_reply := DoneTaskReply{}
				Done_args.Task_t = MapTask
				Done_args.Task_id = task_id
				ok := call("Coordinator.TaskDone", &Done_args, &Done_reply)
				if !ok {
					log.Fatal("call map done error!")
				}
			}(reply.Task_id)

		} else if reply.Task_t == ReduceTask {
			all_data := make(map[string][]string)
			for i := 0; i < reply.Map_task_num; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Task_id)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					all_data[kv.Key] = append(all_data[kv.Key], kv.Value)
				}
				file.Close()
			}

			reduce_out := []KeyValue{}
			for k, v := range all_data {
				tmp_data := KeyValue{k, reducef(k, v)}
				reduce_out = append(reduce_out, tmp_data)
			}

			out_put_file := "mr-out-" + strconv.Itoa(reply.Task_id)
			ofile, err := ioutil.TempFile("", out_put_file)
			if err != nil {
				log.Fatal(err)
			}

			for _, data := range reduce_out {
				fmt.Fprintf(ofile, "%v %v\n", data.Key, data.Value)
				if err != nil {
					log.Fatal(err)
				}
			}
			ofile.Close()
			os.Rename(ofile.Name(), out_put_file)

			go func(task_id int) {
				args := DoneTaskArgs{}
				reply := DoneTaskReply{}
				args.Task_id = task_id
				args.Task_t = ReduceTask
				ok := call("Coordinator.TaskDone", &args, &reply)
				if !ok {
					log.Fatal("reduce call taskdone error!")
				}
			}(reply.Task_id)
		} else if reply.Task_t == WaitTask {
			time.Sleep(1 * time.Second)
		} else {
			fmt.Println("unrecognized task type!")
			os.Exit(1)
		}
	}
	// uncomment(取消注释) to send the Example RPC to the coordinator.
	// CallExample()
}


// 示例函数，展示如何对协调器进行 RPC 调用。RPC 参数和回复类型在 rpc.go 中定义。
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// 发送 RPC 请求，等待回复。"Coordinator.Example" 告诉接收服务器我们想要调用 Coordinator 结构体的 Example() 方法
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}


// 发送一个 RPC 请求到协调器，等待响应。通常情况下返回 true。如果发生错误，则返回 false
//args 和 reply 的类型为 interface{}，表示它们可以接受任何类型的值。这里的 interface{} 后面有一个空的大括号 {}，这是一个空接口类型的定义,声明时{}不可去掉。
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {//nil相当于c++中的nullptr
		log.Fatal("dialing:", err)
	}
	defer c.Close()//defer关键字用于延迟执行c.Close(),将此执行推迟到当前函数返回之前执行.

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
