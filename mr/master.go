package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

const MAP = 0
const REDUCE = 1
const WAIT = 2
const MAPSUCCESS = 3
const FAILED = 4
const MAPDONE = 5
const REDUCESUCCESS = 6

type Master struct {
	// Your definitions here.
	nReduce        int
	mMap           int
	mapFiles       []string
	reduceFiles    []string
	mapJob         []JobInfo //正在做map的文件
	reduceJob      []JobInfo //正在做reduce的文件
	mapFinished    int
	reduceFinished int
}

type JobInfo struct {
	JobType  int //MAP,REDUCE,WAIT
	JobTime  time.Time
	FileName string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) DoJob(args *WorkerMsg, reply *MasterMsg) error {
	job := args.Message
	fileName := job.FileName
	jobTime := job.JobTime
	jobType := job.JobType
	//如果类型是完成,把任务从正在做的队列中删除，同时完成计数器+1
	if args.Message.JobType == MAPSUCCESS {
		fmt.Println(fileName, "任务完成")
		for i := 0; i < len(m.mapJob); i++ {
			fmt.Println(m.mapJob[i].FileName)
			if m.mapJob[i].FileName == fileName {
				//从mapjob中删除该任务
				m.mapJob = append(m.mapJob[:i], m.mapJob[i+1:]...)
				m.mapFinished++
			}
			fmt.Println("任务完成数量:", m.mapFinished)
		}
		return nil
	}
	//先完成所有map
	//并不确定是不是这种运作模式，网上有谈到等待map完成之后再reduce
	//也有等到map完成一部分reduce就执行
	//所以我用了简单的方式 ;-D
	//要同时mapFiles为空且mapFinished 为 M
	if len(m.mapFiles) != 0 && jobType == WAIT { //如果还有map任务就做map任务
		reply.Message.FileName = m.mapFiles[0]
		fmt.Println("map job :", reply.Message.FileName)
		reply.Message.JobTime = time.Now()
		reply.Message.JobType = MAP
		//加入队列
		curJob := JobInfo{}
		curJob.JobType = reply.Message.JobType
		curJob.FileName = reply.Message.FileName
		curJob.JobTime = reply.Message.JobTime
		m.mapJob = append(m.mapJob, curJob)
		m.mapFiles = m.mapFiles[1:]
		return nil
	}
	//fmt.Println(m.mapJob)
	//map任务队列为空但是还没有做完，就让worker等待
	if len(m.mapJob) != 0 && m.mapFinished != m.mMap {
		fmt.Println(len(m.mapFiles), m.mapFinished, m.mMap)
		reply.Message.FileName = "MAP JOB IS  EMPTY"
		reply.Message.JobTime = time.Now()
		reply.Message.JobType = WAIT
		return nil
	}
	//所有任务完成worker退出
	if len(m.mapFiles) == 0 && m.mapFinished == m.mMap {
		reply.Message.JobType = MAPDONE
	}

	fmt.Println(fileName, jobTime, jobType)

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.mMap = len(files)
	m.mapFiles = files
	m.mapFinished = 0
	m.reduceFinished = 0
	m.server()
	return &m
}
