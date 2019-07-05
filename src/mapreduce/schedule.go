package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var work_ch = make(chan int)
	var wg sync.WaitGroup

	go func() {
		for i:=0; i<ntasks; i++ {
			wg.Add(1)
			work_ch<-i
		}
		wg.Wait()
		close(work_ch)
	}()

	task := DoTaskArgs{
		JobName:jobName,
		NumOtherPhase:n_other,
		Phase:phase,
	}

	for task_id := range work_ch {

		s := <-registerChan
		task.TaskNumber = task_id

		if phase == mapPhase {
			task.File = mapFiles[task_id]
		}

		go func(worker string, task DoTaskArgs) {
			if call(worker, "Worker.DoTask", &task, nil) {
				wg.Done()
				registerChan<-worker
			} else {
				work_ch<-task.TaskNumber
			}
		}(s, task)
	}

	// why?
	// wg.Wait()
	// close(work_ch)

	fmt.Printf("Schedule: %v done\n", phase)
}
