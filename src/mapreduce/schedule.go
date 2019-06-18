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
	isMap := false
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		isMap = true
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
	var wg sync.WaitGroup

	// 预先设置协程数量值，如果在协程内执行Add，会导致wg.wait()提前执行
	wg.Add(ntasks)

	for i:=0; i<ntasks; i++ {
		taskArgs := DoTaskArgs{
			JobName: jobName,
			Phase: phase,
			TaskNumber: i,
			NumOtherPhase: n_other,
		}
		if isMap{
			taskArgs.File = mapFiles[i]
		}
		go send(&wg, taskArgs, registerChan)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

func send(wg *sync.WaitGroup, taskArgs DoTaskArgs, registerChan chan string) {
	address := <- registerChan
	if !call(address, "Worker.DoTask", taskArgs, nil) {
		wg.Add(1)
		go send(wg, taskArgs, registerChan)
	}
	// 此处channel为非缓冲通道，故获取与注入同时发生，否则将发生阻塞
	// 再者，多defer遵从后进先出，故以下两个defer位置互换后会发生阻塞在address注入管道过程，不会执行到wg.Done()
	// 最终导致整个程序阻塞
	defer func() {registerChan <- address}()
	defer wg.Done()
}
