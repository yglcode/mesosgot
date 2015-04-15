// +build elevator-sched

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	log "github.com/golang/glog"
	sched "github.com/mesos/mesos-go/scheduler"
	got "github.com/yglcode/mesosgot"
	"time"
)

//TaskMsg and encode/decode defined in elevator.go

const (
	TimeWaitForTasksUp = 5 * time.Second
)

type ElevatorScheduler struct {
	tasks map[string]bool
}

func NewElevatorScheduler() *ElevatorScheduler {
	return &ElevatorScheduler{make(map[string]bool)}
}

func (es *ElevatorScheduler) TasksResourceInfo() []*got.AppTaskResourceInfo {
	return []*got.AppTaskResourceInfo{
		&got.AppTaskResourceInfo{
			Name:        "elevator",
			Count:       DefaultNumberOfElevators,
			CpusPerTask: 0.1,
			MemPerTask:  96,
		},
		&got.AppTaskResourceInfo{
			Name:        "floor",
			Count:       DefaultNumberOfFloors,
			CpusPerTask: 0.1,
			MemPerTask:  96,
		},
	}
}

func (es *ElevatorScheduler) taskCount() (count int) {
	for _, ri := range es.TasksResourceInfo() {
		count += ri.Count
	}
	return
}

//scheduler will run in its own goroutine, communicate with tasks thru chans
func (es *ElevatorScheduler) RunScheduler(schedin <-chan got.GoTaskMsg, schedout chan<- got.GoTaskMsg, schedevent <-chan got.SchedEvent) {
	//init scheduler
	sched := NewScheduler(DefaultNumberOfFloors, DefaultNumberOfElevators, schedin, schedout)
	var msg schedMsg
	taskCount := es.taskCount()
	//first wait for tasks come up
waitloop:
	for i := 0; i < taskCount; i++ {
		select {
		case taskMsg := <-schedin:
			err := msg.decode(taskMsg)
			if err != nil {
				continue
			}
			if msg.currFloor != 0 || msg.goalFloor != 0 {
				continue
			}
			es.tasks[msg.taskName] = true
		case <-time.After(TimeWaitForTasksUp):
			break waitloop
		}
	}
	//tell all tasks start
	msg.currFloor = 0
	msg.goalFloor = 0
	for tname, _ := range es.tasks {
		log.Infoln("notify: ", tname)
		msg.taskName = tname
		schedout <- msg.encode()
	}
	//run scheduler
	sched.Run()
	//tell all tasks stop
	msg.currFloor = -1
	msg.goalFloor = -1
	for tname, _ := range es.tasks {
		log.Infoln("notify: ", tname)
		msg.taskName = tname
		schedout <- msg.encode()
	}
	//wait for all tasks exit
msgloop:
	for m := range schedin {
		err := msg.decode(m)
		if err != nil {
			log.Infoln("failed decode msg: ", err)
			continue
		}
		switch {
		case msg.currFloor == -1 && msg.goalFloor == -1:
			log.Infoln("finished task: ", msg.taskName)
			delete(es.tasks, msg.taskName)
			if len(es.tasks) == 0 {
				break msgloop
			}
		default:
			log.Infof("recv %v\n", msg)
		}
	}
}

// ----------------------- func main() ------------------------- //

func main() {

	schedConfig := got.LoadSchedulerConfig()

	schd := got.NewGoTaskScheduler(
		"", //userName, Mesos-go will fill in user.
		NewElevatorScheduler(),
		schedConfig)

	driver, err := sched.NewMesosSchedulerDriver(schd.DriverConfig())

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}

}
