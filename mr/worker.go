package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := WorkerMsg{}
		args.Message.JobType = WAIT
		args.Message.JobTime = time.Now()
		args.Message.FileName = ""
		reply := MasterMsg{}
		call("Master.DoJob", &args, &reply)
		fmt.Println(reply.Message.FileName, reply.Message.JobTime, reply.Message.JobType)
		//获得了master的答复

		if reply.Message.JobType == WAIT {
			time.Sleep(time.Second)
			fmt.Println("wait")
			continue
		}
		//mapdojob
		if reply.Message.JobType == MAP {
			fmt.Println("mapping")
		}

		args.Message.JobType = MAPSUCCESS
		args.Message.JobTime = time.Now()
		args.Message.FileName = reply.Message.FileName
		//设置map参数
		call("Master.DoJob", &args, &reply)

		if reply.Message.JobType == MAPDONE {
			break
		}

	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
