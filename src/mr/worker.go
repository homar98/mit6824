package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KVList []KeyValue

func (a KVList) Len() int           { return len(a) }
func (a KVList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KVList) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		task_request_args := TaskRequestArgs{}
		task_request_reply := TaskRequestReply{}
		connected := SendTaskRequestSignal(&task_request_args, &task_request_reply)

		if !connected {
			log.Printf("Fail to send task request signal!")
		} else if task_request_reply.Type == 3 {
			log.Printf("Worker is told to exit!")
			break
		} else if task_request_reply.Type == 2 { // Worker stand by
			log.Printf("No task assigned! Standing by...")
		} else if task_request_reply.Type == 0 { // Map tasks
			MapTaskExecution(&task_request_reply, mapf)
		} else if task_request_reply.Type == 1 { // Reduce tasks
			ReduceTaskExecution(&task_request_reply, reducef)
		}

		time.Sleep(time.Second)
	}
	return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func SendTaskRequestSignal(args *TaskRequestArgs, reply *TaskRequestReply) bool {
	return call("Coordinator.TaskRequestHandler", &args, &reply)
}

func SendTaskFinishSignal(args *TaskFinishArgs, reply *TaskFinishReply) bool {
	return call("Coordinator.TaskFinishHandler", &args, &reply)
}

func WriteIntermediateFile(kv_list []KeyValue, id_map int, id_reduce int) bool {
	intermediate_filename := fmt.Sprintf("mr-%v-%v", id_map, id_reduce)
	file, err := ioutil.TempFile("./", intermediate_filename)
	if err != nil {
		log.Printf("Map task %v: Fail to create intermediate file: %v!", id_map, intermediate_filename)
		return false
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(kv_list)

	if err != nil {
		log.Printf("Map task %v: Fail to encode json in file: %v", id_map, intermediate_filename)
		return false
	}

	os.Rename(file.Name(), intermediate_filename)
	return true
}

func MapTaskExecution(task_request_reply *TaskRequestReply, mapf func(string, string) []KeyValue) {
	id_map := task_request_reply.Id_map_task
	num_reduce := task_request_reply.Num_reduce
	filename := task_request_reply.Message

	log.Printf("Launching map task %v for input file %v", id_map, filename)

	// Reading input file
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Map task %v: Cannot open %v! Task is aborted!", id_map, filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("Map task %v: Cannot read %v! Task is aborted", id_map, filename)
		file.Close()
		return
	}
	file.Close()

	// Running user-defined map function
	kv_list := mapf(filename, string(content))

	// Partitioning
	kv_partition_list := make([][]KeyValue, num_reduce)
	for _, kv := range kv_list {
		id_reduce := ihash(kv.Key) % num_reduce
		kv_partition_list[id_reduce] = append(kv_partition_list[id_reduce], kv)
	}

	// Write intermediate files
	written := true
	for i := 0; i < num_reduce; i++ {
		sort.Sort(KVList(kv_partition_list[i]))
		written = WriteIntermediateFile(kv_partition_list[i], id_map, i)
		if !written {
			break
		}
	}
	if !written {
		log.Printf("Map task %v: Fail to write intermediate files! Task is aborted!", id_map)
		return
	}
	log.Printf("Map task %v: Output has been written to files! Informing master...", id_map)

	// Inform master task has been finished
	task_finish_args := TaskFinishArgs{Id_map_task: id_map, Type: 0}
	task_finish_reply := TaskFinishReply{Ack: false}
	ret := SendTaskFinishSignal(&task_finish_args, &task_finish_reply)
	if !ret {
		log.Printf("Map task %v: Fail to send task finish signal to master! Task is aborted!", id_map)
		return
	}

	if task_finish_reply.Ack {
		log.Printf("Map task %v: Task has been acknowledged by master!", id_map)
	} else {
		log.Printf("Map task %v: Task is not acknowledged by master!", id_map)
	}
}

func ReduceTaskExecution(task_request_reply *TaskRequestReply, reducef func(string, []string) string) {
	id_reduce := task_request_reply.Id_reduce_task
	num_map := task_request_reply.Num_map

	log.Printf("Launching reduce task %v...", id_reduce)

	output_filename := fmt.Sprintf("mr-out-%v", id_reduce)
	output_file, err := ioutil.TempFile("./", output_filename)
	if err != nil {
		log.Printf("Reduce task %v: Fail to create output file: %v", id_reduce, output_filename)
	}

	kv_list := []KeyValue{}

	for id_map := 0; id_map < num_map; id_map++ {
		intermediate_filename := fmt.Sprintf("mr-%v-%v", id_map, id_reduce)
		intermediate_file, err := os.Open(intermediate_filename)

		if err != nil {
			log.Printf("Reduce task %v: Fail to open intermediate file %v! Task is aborted!", id_reduce, intermediate_filename)
			return
		}

		kv_partition_list := []KeyValue{}
		decoder := json.NewDecoder(intermediate_file)
		err = decoder.Decode(&kv_partition_list)

		if err != nil {
			log.Printf("Reduce task %v: Fail to decode json in file: %v", id_reduce, intermediate_filename)
			return
		}

		kv_list = append(kv_list, kv_partition_list...)
	}

	sort.Sort(KVList(kv_list))
	i := 0
	for i < len(kv_list) {
		j := i + 1
		for j < len(kv_list) && kv_list[j].Key == kv_list[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv_list[k].Value)
		}
		output := reducef(kv_list[i].Key, values)
		fmt.Fprintf(output_file, "%v %v\n", kv_list[i].Key, output)

		i = j
	}

	os.Rename(output_file.Name(), output_filename)
	log.Printf("Reduce task %v: Output has been written to file: %v! Informing master...", id_reduce, output_filename)

	task_finish_args := TaskFinishArgs{Id_reduce_task: id_reduce, Type: 1}
	task_finish_reply := TaskFinishReply{Ack: false}
	ret := SendTaskFinishSignal(&task_finish_args, &task_finish_reply)
	if !ret {
		log.Printf("Reduce task %v: Fail to send task finish signal to master! Task is aborted!", id_reduce)
	}

	if task_finish_reply.Ack {
		log.Printf("Reduce task %v: Task has been acknowledged by master!", id_reduce)
	} else {
		log.Printf("Reduce task %v: Task is not acknowledged by master!", id_reduce)
	}
}
