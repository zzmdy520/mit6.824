package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"

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
	var f [16]*os.File
	var err error
	for i := 0; i < 10; i++ {
		oname := "Reduce_" + strconv.Itoa(i) + ".txt"
		fmt.Println(oname)
		f[i], err = os.OpenFile(oname, os.O_APPEND|os.O_RDWR, 0777) //打开文件
		if err != nil {
			fmt.Println(err)
		}
	}
	// Your worker implementation here.
	//打开文件

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

			filename := reply.Message.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content)) //filename在mapf里没用到 返回 []mr.KeyValue
			//fmt.Println(string(content))
			for i, kv := range kva {
				n := ihash(kv.Key) % 10
				fmt.Fprintf(f[n], "%v %v\n", kva[i].Key, kva[i].Value)
				//fmt.Println(n, kva[i].Key, kva[i].Value)
			}
		}
		//mapdojob结束
		//反馈信息 注意10s超时的限制
		time.Sleep(time.Second)
		args.Message.JobType = MAPSUCCESS
		args.Message.JobTime = time.Now()
		args.Message.FileName = reply.Message.FileName
		//设置map参数
		call("Master.DoJob", &args, &reply)

		//reduce任务
		if reply.Message.JobType == MAPDONE {
			break
		}

	}

	//关闭文件
	for i := 0; i < 10; i++ {
		f[i].Close()

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
