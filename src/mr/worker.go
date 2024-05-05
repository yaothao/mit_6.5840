package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	file.Close()
	return string(content)

}

func writeToIntermediate(intermediateBuckets map[int][]KeyValue, taskId int) error {
	for k, v := range intermediateBuckets {
		sort.Sort(ByKey(v))
		tmpFile, err := ioutil.TempFile("", "temp-*")
		defer tmpFile.Close()
		if err != nil {
			log.Fatal(err)
		}

		oname := fmt.Sprintf("mr-%v-%v", taskId, k)
		enc := json.NewEncoder(tmpFile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("error in encoding kv, %v , %v\n", taskId, k)
				os.Remove(tmpFile.Name())
				return err
			}
		}

		os.Rename(tmpFile.Name(), oname)
		// fmt.Printf("Intermediate Buckets recorded, %v , %v\n", taskId, k)
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// map
	// ask for a task from the coordinator
	for {
		filenames, taskName, taskId, nReduce, reduceFile := getTask()
		if taskName == "Map" {
			filename := filenames[0]
			intermediate := []KeyValue{}
			kva := mapf(filename, mapFile(filename))
			intermediate = append(intermediate, kva...)
			intermediateBuckets := make(map[int][]KeyValue)
			for _, v := range intermediate {
				reduceId := ihash(v.Key) % nReduce
				intermediateBuckets[reduceId] =
					append(intermediateBuckets[reduceId], v)
			}

			if submitTask(filename, taskId) {
				err := writeToIntermediate(intermediateBuckets, taskId)
				if err != nil {
					fmt.Printf("Failure: %v \n", err)
				} else {
					submitTask(filename, taskId)
				}
			}
		} else if taskName == "Reduce" {
			KVCollect := []KeyValue{}
			oname := fmt.Sprintf("mr-out-%v", taskId)
			tempOfile, _ := ioutil.TempFile("", "mr-out-*")
			defer tempOfile.Close()
			pattern := fmt.Sprintf("mr-*-%v", reduceFile)
			files, err := filepath.Glob(pattern)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			for _, file := range files {
				// fmt.Printf("working on reduce file: %v", file)
				f, err := os.Open(file)
				if err != nil {
					break
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					KVCollect = append(KVCollect, kv)
				}
			}

			sort.Sort(ByKey(KVCollect))
			i := 0
			//fmt.Printf("working on count reduce file\n")
			for i < len(KVCollect) {
				j := i + 1
				for j < len(KVCollect) && KVCollect[j].Key == KVCollect[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, KVCollect[k].Value)
				}

				output := reducef(KVCollect[i].Key, values)

				fmt.Fprintf(tempOfile, "%v %v\n", KVCollect[i].Key, output)
				i = j
			}

			if submitReduceTask(reduceFile, taskId) {
				os.Rename(tempOfile.Name(), oname)
				submitReduceTask(reduceFile, taskId)
			} else {
				os.Remove(tempOfile.Name())
			}

		} else if taskName == "Terminate" {
			os.Exit(0)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func getTask() ([]string, string, int, int, int) {
	args := GetArgs{}
	reply := GetReply{}
	//time.Sleep(time.Second * 3)
	ok := call("Coordinator.GetTask", &args, &reply)
	//time.Sleep(time.Second * 10)
	if ok {
		//fmt.Printf("Debug---filename:%v, reducefile:%v, taskid:%v, taskName:%v\n", reply.Filenames, reply.ReduceFile, reply.TaskId, reply.TaskName)
	} else {
		fmt.Printf("Worker Request Failed\n")
	}
	return reply.Filenames, reply.TaskName, reply.TaskId, reply.NReduce, reply.ReduceFile
}

// call and notify the coordinator once the task in finished by the worker

func submitTask(filename string, taskId int) bool {
	args := SubmitArgs{Filename: filename, TaskId: taskId}
	reply := SubmitReply{}

	ok := call("Coordinator.SubmitTask", &args, &reply)
	if !ok || !reply.Ok {
		fmt.Printf("Task submit failed\n")
		return false
	}
	return true
}

// call and notify the coordinator once the task in finished by the worker

func submitReduceTask(reduceFile int, taskId int) bool {
	args := SubmitArgs{ReduceFile: reduceFile, TaskId: taskId}
	reply := SubmitReply{}

	ok := call("Coordinator.SubmitReduceTask", &args, &reply)
	if !ok || !reply.Ok {
		fmt.Printf("Task submit failed\n")
		return false
	}
	return true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
