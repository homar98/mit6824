package mr

import (
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
	input_files        []string
	map_task_states    []int
	reduce_task_states []int
	map_done           bool
	reduce_done        bool
	num_reduce         int
	num_map            int
	mux                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) TaskRequestHandler(args *TaskRequestArgs, reply *TaskRequestReply) error {
	log.Printf("Received task request from worker!")
	c.mux.Lock()

	if c.map_done && c.reduce_done {
		log.Printf("All tasks is done! Informing worker to exit...")
		reply.Type = 3
	} else if !c.map_done {
		id_map := -1
		for i := 0; i < c.num_map; i++ {
			if c.map_task_states[i] == 0 {
				id_map = i
				break
			}
		}
		if id_map == -1 {
			reply.Type = 2
			log.Printf("No map task is available now! Informing worker to stand by...")
		} else {
			reply.Type = 0
			reply.Id_map_task = id_map
			reply.Num_reduce = c.num_reduce
			reply.Message = c.input_files[id_map]
			c.map_task_states[id_map] = 1
			log.Printf("Assigning map task %v with input file %v to worker...", id_map, c.input_files[id_map])

			// Start a goroutine to monitor task state
			go c.MonitorMapTaskState(id_map)
		}
	} else if !c.reduce_done {
		id_reduce := -1
		for i := 0; i < c.num_reduce; i++ {
			if c.reduce_task_states[i] == 0 {
				id_reduce = i
				break
			}
		}
		if id_reduce == -1 {
			log.Printf("No reduce task is available now! Informing worker to stand by...")
			reply.Type = 2
		} else {
			reply.Type = 1
			reply.Id_reduce_task = id_reduce
			reply.Num_map = c.num_map
			c.reduce_task_states[id_reduce] = 1
			log.Printf("Assigning reduce task %v to worker...", id_reduce)

			// Start a goroutine to monitor reduce task state
			go c.MonitorReduceTaskState(id_reduce)
		}
	}
	c.mux.Unlock()

	return nil
}

// RPC handler dealing with task finish signal from worker
func (c *Coordinator) TaskFinishHandler(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mux.Lock()
	if args.Type == 0 && c.map_task_states[args.Id_map_task] == 1 {
		log.Printf("Received map task %v finish signal from worker!", args.Id_map_task)
		c.map_task_states[args.Id_map_task] = 2
	} else if args.Type == 1 && c.reduce_task_states[args.Id_reduce_task] == 1 {
		log.Printf("Received reduce task %v finish signal from worker!", args.Id_reduce_task)
		c.reduce_task_states[args.Id_reduce_task] = 2
	}

	// Check Map/Reduce done state
	c.map_done = true
	for i := 0; i < c.num_map; i++ {
		if c.map_task_states[i] != 2 {
			c.map_done = false
			break
		}
	}

	c.reduce_done = true
	for i := 0; i < c.num_reduce; i++ {
		if c.reduce_task_states[i] != 2 {
			c.reduce_done = false
			break
		}
	}

	c.mux.Unlock()
	reply.Ack = true
	return nil
}

// Check map task state.
// Running on a single goroutine.
// An unfinished task after 10 seconds will be unassigned again.
func (c *Coordinator) MonitorMapTaskState(id_map int) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		c.mux.Lock()
		state := c.map_task_states[id_map]
		c.mux.Unlock()
		if state == 2 {
			return
		}
	}
	log.Printf("Assigned map task %v is not finished in 10 seconds! Task will be re-assigned!", id_map)
	c.mux.Lock()
	c.map_task_states[id_map] = 0
	c.mux.Unlock()
}

func (c *Coordinator) MonitorReduceTaskState(id_reduce int) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		c.mux.Lock()
		state := c.reduce_task_states[id_reduce]
		c.mux.Unlock()
		if state == 2 {
			return
		}
	}
	log.Printf("Assigned reduce task %v is not finished in 10 seconds! Task will be re-assigned!", id_reduce)
	c.mux.Lock()
	c.reduce_task_states[id_reduce] = 0
	c.mux.Unlock()
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mux.Lock()
	ret = c.map_done && c.reduce_done
	c.mux.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{input_files: files,
		map_task_states:    make([]int, len(files)),
		reduce_task_states: make([]int, nReduce),
		map_done:           false,
		reduce_done:        false,
		num_reduce:         nReduce,
		num_map:            len(files)}
	// Your code here.

	c.server()
	return &c
}
