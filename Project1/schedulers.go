package main

import (
	"container/heap"
	"fmt"
	"io"
)

type (
	Process struct {
		ProcessID     string
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   string
		Start int64
		Stop  int64
	}
)

type Element struct {
	process      Process
	index        int
	waitTime     int64
	timeWorked   int64
	currentSlice int64
}

// shamelessly copied from the Go docs example
type SJFQueue []*Element

func (pq SJFQueue) Len() int { return len(pq) }

func (pq SJFQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return (pq[i].process.BurstDuration - pq[i].timeWorked) < (pq[j].process.BurstDuration - pq[j].timeWorked)
}

func (pq SJFQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *SJFQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Element)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *SJFQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *SJFQueue) update(item *Element, burst int64) {
	item.process.BurstDuration = burst
	heap.Fix(pq, item.index)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// assumes processes[] is already sorted by arrival time
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		turnaround     float64
		throughput     float64
		aveWaitTime    float64
		totalTime      int64 = 0
		start          int64 = 0
		currentProcess *Element
		schedule       = make([][]string, len(processes))
		gantt          = make([]TimeSlice, 0)
	)
	// initialize priority queue
	pq := make(SJFQueue, 0)
	heap.Init(&pq)

	// insert processes into queue and work on them at the same time
	for i := range processes {
		// time between next process arrival and the current time
		elapsedTime := processes[i].ArrivalTime - totalTime
		// loop until current time is the same as the process's arrival time
		for elapsedTime > 0 {
			if currentProcess == nil { /** this is true when one of the following is true:
						a. i = 0... have not added or worked any processes yet
						b. all processes finished when adding the previous process
						c. the current process finished but there's still time left before ArrivalTime
				**/
				if len(pq) == 0 { // no more processes to work on, so update time to arrival time and exit loop
					totalTime = processes[i].ArrivalTime
					break
				}
				// dispatch process and update start time
				currentProcess = heap.Pop(&pq).(*Element)
				start = totalTime
			}
			// timeUsed is the time the current process can use... restricted by time between ArrivalTime and current time, and the amount of burst left on the process
			timeUsed := min(elapsedTime, GetRemainingBurst(*currentProcess))
			currentProcess.timeWorked += timeUsed
			totalTime += timeUsed   // update current time with the amount of CPU time used
			elapsedTime -= timeUsed // decrease remaining time until ArrivalTime
			if IsProcessComplete(*currentProcess) {
				ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, currentProcess, start, totalTime)
				currentProcess = nil
			}
		}
		// if we don't have a process to preempt, or the arrival doesn't preempt, push arrival to queue
		if currentProcess == nil || processes[i].BurstDuration >= GetRemainingBurst(*currentProcess) {
			heap.Push(&pq, &Element{process: processes[i]})
		} else { // else process is preempted
			UpdateGantt(&gantt, *currentProcess, start, totalTime)
			heap.Push(&pq, currentProcess)
			currentProcess = &(Element{process: processes[i]})
			start = totalTime
		}
	}
	// no more processes to add, so start working on them
	// since there are no more processes, we don't care about preemption anymore
	for len(pq) > 0 {
		if currentProcess == nil { // this can only be false at the start... code looks neater than copying 198-201 outside the for loop and in an if statement
			currentProcess = heap.Pop(&pq).(*Element)
			start = totalTime
		}
		totalTime += GetRemainingBurst(*currentProcess)
		currentProcess.timeWorked = currentProcess.process.BurstDuration
		ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, currentProcess, start, totalTime)
		currentProcess = nil
	}

	// generic stat calcs and outputting
	turnaround /= float64(len(processes))
	aveWaitTime /= float64(len(processes))
	throughput = float64(len(processes)) / float64(totalTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWaitTime, turnaround, throughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var turnaround, throughput, aveWaitTime float64
	var totalTime, start int64 = 0, 0
	var currentProcess *Element
	var (
		schedule = make([][]string, len(processes))
		gantt    = make([]TimeSlice, 0)
		pq       = make([]SJFQueue, 50) // index is based on the process's priority - 1 (because arrays start at 0 but priorities start at 1)
	)
	for i := range pq {
		pq[i] = make(SJFQueue, 0)
		heap.Init(&(pq[i]))
	}

	for i := range processes {
		// elapsedTime refers to the amount of time between "now" and the arrival of the next process
		elapsedTime := processes[i].ArrivalTime - totalTime
		for elapsedTime > 0 { // we want to keep removing the highest priority process until we have no more time
			if currentProcess == nil { // since we aren't already acting on a process, we need to get a new one
				currentProcess = PopFirst(&pq)
				start = totalTime // also update the start time
			}
			if currentProcess == nil { // set time to latest time since no processes exist to execute until latest arrival
				totalTime = processes[i].ArrivalTime
				break // while setting elapsedTime to 0 gives the same result, it results in unnecessary operations and checks
			}
			// timeUsed refers to the amount of time used for this process between the current time and arrival time of the next process
			timeUsed := min(elapsedTime, GetRemainingBurst(*currentProcess))
			currentProcess.timeWorked += timeUsed
			totalTime += timeUsed
			elapsedTime -= timeUsed
			if IsProcessComplete(*currentProcess) {
				ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, currentProcess, start, totalTime)
				//start = totalTime // we want to update start time only when we remove a process to start work on it
				currentProcess = PopFirst(&pq)
				start = totalTime
			}
		}
		if currentProcess == nil { // the only time it should be nil is if there were no processes in any of the queues
			// so, we can set currentProcess to the process that just arrived
			currentProcess = &(Element{process: processes[i]})
			start = totalTime
		} else if processes[i].Priority < currentProcess.process.Priority || // check if new process has higher priority
			(processes[i].Priority == currentProcess.process.Priority && // check if same priority but
				processes[i].BurstDuration < GetRemainingBurst(*currentProcess)) { // shorter job duration
			// newest process preempts currentProcess
			if start != totalTime {
				UpdateGantt(&gantt, *currentProcess, start, totalTime)
				start = totalTime
			}
			heap.Push(&(pq[currentProcess.process.Priority-1]), currentProcess)
			currentProcess = &(Element{process: processes[i]})

		} else { // process priority is lower than current process, or it's equal but job time isn't shorter
			// push it into its appropriate queue
			heap.Push(&(pq[processes[i].Priority-1]), &(Element{process: processes[i]}))
		}
	}
	// assuming 314-322 work as intended, no other process should preempt currentProcess
	// so check if it's there and work it until it's done
	if currentProcess != nil {
		totalTime += GetRemainingBurst(*currentProcess)
		currentProcess.timeWorked = currentProcess.process.BurstDuration
		ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, currentProcess, start, totalTime)
		// don't need to set to nil since first operation should be getting the first element
	}
	// we use a new for loop instead of repeatedly calling PopFirst() so that we don't have to recheck every single previous queue
	for i := range pq {
		for len(pq[i]) > 0 {
			currentProcess = heap.Pop(&(pq[i])).(*Element)
			start = totalTime
			totalTime += GetRemainingBurst(*currentProcess)
			currentProcess.timeWorked = currentProcess.process.BurstDuration
			ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, currentProcess, start, totalTime)
		}
	}

	turnaround /= float64(len(processes))
	aveWaitTime /= float64(len(processes))
	throughput = float64(len(processes)) / float64(totalTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWaitTime, turnaround, throughput)

}

func RRSchedule(w io.Writer, title string, processes []Process) {
	//implement a queue via a linked list

	var (
		timeSliceSize  int64 = 4
		queue                = make([]Element, 0)
		totalTime      int64 = 0
		start          int64 = 0
		turnaround     float64
		throughput     float64
		aveWaitTime    float64
		schedule       = make([][]string, len(processes))
		gantt          = make([]TimeSlice, 0)
		currentProcess Element
	)

	for i := range processes {
		if len(queue) == 0 {
			totalTime = processes[i].ArrivalTime
			start = totalTime
			Push(&queue, Element{process: processes[i]})
			continue
		}
		if currentProcess == (Element{}) {
			currentProcess = Pop(&queue)
		}
		elapsedTime := processes[i].ArrivalTime - totalTime
		for elapsedTime > 0 {
			// the time a process can use is limited by the time left, size of time slice, amount of remaining slice, and amount of time needed to finish the process
			// therefore, we select the limiting factor
			timeUsed := min(min(elapsedTime, timeSliceSize-currentProcess.currentSlice), GetRemainingBurst(currentProcess))
			currentProcess.timeWorked += timeUsed
			currentProcess.currentSlice += timeUsed
			elapsedTime -= timeUsed
			totalTime += timeUsed
			if IsProcessComplete(currentProcess) {
				ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, &currentProcess, start, totalTime)
				if len(queue) > 0 {
					currentProcess = Pop(&queue)
					start = totalTime
				} else {
					currentProcess = Element{}
					totalTime = processes[i].ArrivalTime
					start = totalTime
					Push(&queue, Element{process: processes[i]})
					break
				}
			} else if currentProcess.currentSlice == timeSliceSize { // current process can be put at end of queue
				currentProcess.currentSlice = 0
				UpdateGantt(&gantt, currentProcess, start, totalTime)
				if totalTime == processes[i].ArrivalTime {
					Push(&queue, Element{process: processes[i]})
				}
				Push(&queue, currentProcess)
				currentProcess = Pop(&queue)
				start = totalTime
			} else if elapsedTime == 0 {
				Push(&queue, Element{process: processes[i]})
			}
		}
	}

	for {
		timeUsed := min(timeSliceSize-currentProcess.currentSlice, GetRemainingBurst(currentProcess))
		currentProcess.timeWorked += timeUsed
		totalTime += timeUsed
		if IsProcessComplete(currentProcess) {
			ProcessFinishedUpdate(&schedule, &gantt, &turnaround, &aveWaitTime, &currentProcess, start, totalTime)
			if len(queue) > 0 {
				currentProcess = Pop(&queue)
				start = totalTime
			} else {
				break
			}
		} else /*if currentProcess.currentSlice == timeSliceSize */ { // this condition is implied by previous being false
			UpdateGantt(&gantt, currentProcess, start, totalTime)
			Push(&queue, currentProcess)
			currentProcess = Pop(&queue)
			start = totalTime
		}
	}

	turnaround /= float64(len(processes))
	aveWaitTime /= float64(len(processes))
	throughput = float64(len(processes)) / float64(totalTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWaitTime, turnaround, throughput)
}

//endregion

// helper funcs
func TurnaroundTime(element Element) int64 {
	return element.waitTime + element.process.BurstDuration
}

func IsProcessComplete(element Element) bool {
	return element.timeWorked == element.process.BurstDuration
}

// ProcessFinishedUpdate Updates the wait time for the process, and average stats like turnaround, average wait time. Also adds the process to the schedule and gantt chart
func ProcessFinishedUpdate(schedule *[][]string, gantt *[]TimeSlice, turnaround *float64, aveWaitTime *float64, currentProcess *Element, start int64, end int64) {
	(*currentProcess).waitTime = end - (*currentProcess).process.ArrivalTime - (*currentProcess).process.BurstDuration
	*turnaround += float64(TurnaroundTime(*currentProcess))
	*aveWaitTime += float64((*currentProcess).waitTime)
	//remove process
	*schedule = append(*schedule, []string{
		fmt.Sprint((*currentProcess).process.ProcessID),
		fmt.Sprint((*currentProcess).process.Priority),
		fmt.Sprint((*currentProcess).process.BurstDuration),
		fmt.Sprint((*currentProcess).process.ArrivalTime),
		fmt.Sprint((*currentProcess).waitTime),
		fmt.Sprint(TurnaroundTime(*currentProcess)),
		fmt.Sprint(end),
	})
	UpdateGantt(gantt, *currentProcess, start, end)
}

// UpdateGantt Update the Gantt chart with values
func UpdateGantt(gantt *[]TimeSlice, currentProcess Element, start int64, end int64) {
	*gantt = append(*gantt, TimeSlice{
		PID:   currentProcess.process.ProcessID,
		Start: start,
		Stop:  end,
	})
}

func GetRemainingBurst(e Element) int64 {
	return e.process.BurstDuration - e.timeWorked
}

func PopFirst(pq *[]SJFQueue) *Element {
	for i := range *pq {
		if len((*pq)[i]) > 0 {
			return heap.Pop(&((*pq)[i])).(*Element)
		}
	}
	return nil
}

func Push(a *[]Element, e Element) {
	*a = append(*a, e)
}

func Pop(a *[]Element) Element {
	e := (*a)[0]
	*a = (*a)[1:]
	return e
}
