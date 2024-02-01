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
	mu sync.Mutex
	// -1 Empty
	// -2 Delegated to worker for write to file ( Submit task for first time )
	// -3 Worker report write to file success ( Submit task for second time )
	fileManager             map[string]int
	intermediateFileManager map[int]int
	todoFile                []string
	retryFile               []string
	reduceFile              []int
	mapId                   int
	reduceId                int
	nReduce                 int
	numFileRemain           int
	numReduceFileRemain     int
	cond                    sync.Cond
}

func (c *Coordinator) makeReply(filename string, taskName string,
	reply *GetReply) *GetReply {
	reply.Filenames = append(reply.Filenames, filename)
	reply.TaskName = taskName
	reply.TaskId = c.mapId
	reply.NReduce = c.nReduce

	return reply
}

// Distribute files to workers
func (c *Coordinator) GetTask(args *GetArgs, reply *GetReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// first test the case if all map tasks are distributed but not 100% completely
	// if so, main thread wait for two options:
	//  	1. one or more map worker tasks failed -> todo file become non zero
	// 		2. all map worker threads 100% finished -> num File remain becomes zero
	if len(c.todoFile) == 0 && c.numFileRemain > 0 {
		for len(c.todoFile) == 0 && c.numFileRemain > 0 {
			c.cond.Wait()
		}
	}

	// The first case falls under to this condition
	if len(c.todoFile) != 0 {
		filename := c.todoFile[0]
		c.todoFile = c.todoFile[1:]
		reply = c.makeReply(filename, "Map", reply)
		c.fileManager[filename] = c.mapId
		c.mapId += 1

		go func(filename string) {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.fileManager[filename] > -2 {
				fmt.Printf("file name %v didn't process\n", filename)
				c.fileManager[filename] = -1
				// c.retryFile = append(c.retryFile, filename)
				c.todoFile = append(c.todoFile, filename)
				c.cond.Broadcast()
			}
		}(filename)
	} else if c.numFileRemain == 0 { // the second condition corresponds with second case

		// Start Reduece Worker

		// Simialr Idea where running a waited loop to see if every Reduce worker finished
		// all the distributed reduce tasks
		if len(c.reduceFile) == 0 && c.numReduceFileRemain > 0 {
			for len(c.reduceFile) == 0 && c.numReduceFileRemain > 0 {
				c.cond.Wait()
			}
		}

		// Case number one if one or more reduce tasks failed -> reduceFile becomes non zero
		if len(c.reduceFile) != 0 {
			reduceFile := c.reduceFile[0]
			c.reduceFile = c.reduceFile[1:]
			reply.ReduceFile = reduceFile
			reply.TaskName = "Reduce"
			reply.TaskId = c.reduceId
			c.intermediateFileManager[reduceFile] = c.reduceId
			c.reduceId++

			go func(reduceFile int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.intermediateFileManager[reduceFile] > -2 {
					fmt.Printf("Reduce file name %v didn't process\n", reduceFile)
					c.intermediateFileManager[reduceFile] = -1
					c.reduceFile = append(c.reduceFile, reduceFile)
					c.cond.Broadcast()
				}

			}(reduceFile)
		}

	}

	// finally test if all the tests are finished, if so terminate the worker threads
	if c.numFileRemain == 0 && c.numReduceFileRemain == 0 {
		//fmt.Printf("Assignining Termination to Worker\n")
		reply.TaskName = "Terminate"
	}

	return nil

}

func (c *Coordinator) SubmitTask(args *SubmitArgs, reply *SubmitReply) error {
	submitFilename := args.Filename
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fileManager[submitFilename] == args.TaskId {
		c.fileManager[submitFilename] = -2
		reply.Ok = true
		//fmt.Printf("filename %v submit task 1 received\n", submitFilename)
		return nil
	} else if c.fileManager[submitFilename] == -2 {
		c.fileManager[submitFilename] = -3
		c.numFileRemain--
		reply.Ok = true
		//fmt.Printf("filename %v submit task 2 received\n", submitFilename)
		c.cond.Broadcast()
		return nil
	}
	reply.Ok = false
	return nil
}

func (c *Coordinator) SubmitReduceTask(args *SubmitArgs, reply *SubmitReply) error {
	reduceNum := args.ReduceFile
	taskId := args.TaskId
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.intermediateFileManager[reduceNum] == taskId {
		c.intermediateFileManager[reduceNum] = -2
		reply.Ok = true
		return nil
	} else if c.intermediateFileManager[reduceNum] == -2 {
		c.intermediateFileManager[reduceNum] = -3
		reply.Ok = true
		c.numReduceFileRemain--
		c.cond.Broadcast()
		return nil
	}
	reply.Ok = false
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.numReduceFileRemain != 0 {
		c.cond.Wait()
	}

	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.fileManager = make(map[string]int)
	c.intermediateFileManager = make(map[int]int)
	c.todoFile = files
	c.mapId = 0
	c.nReduce = nReduce
	c.numFileRemain = len(files)
	c.cond = *sync.NewCond(&c.mu)
	c.reduceFile = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceFile[i] = i
	}
	c.numReduceFileRemain = nReduce

	c.server()
	return &c
}
