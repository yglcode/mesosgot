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

//scheduler will run in its own goroutine, communicate with tasks thru chans
func schedFunc(schedin <-chan got.GoTaskMsg, schedout chan<- got.GoTaskMsg, schedevent <-chan got.SchedEvent) {
	var msg schedMsg
	tasks := make(map[string]bool)
	exitCh := make(chan bool,1)
	taskCount := DefaultNumberOfElevators + DefaultNumberOfFloors
	//run a goroutine to check scheduling events
	go func() {
		schedEventLoop: for {
			select {
			case evt := <- schedevent:
				//just dump it, not real logic
				log.Info(evt)
			case <- exitCh:
				break schedEventLoop
			}
		}
		exitCh<-true
	}()

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
			tasks[msg.taskName] = true
		case <-time.After(TimeWaitForTasksUp):
			break waitloop
		}
	}
	//tell all tasks start
	msg.currFloor = 0
	msg.goalFloor = 0
	for tname, _ := range tasks {
		log.Infoln("notify: ", tname)
		msg.taskName = tname
		schedout <- msg.encode()
	}
	//run scheduler
	//init scheduler
	sched := NewScheduler(DefaultNumberOfFloors, DefaultNumberOfElevators, schedin, schedout)
	sched.Run()
	//tell all tasks stop
	msg.currFloor = -1
	msg.goalFloor = -1
	for tname, _ := range tasks {
		log.Infoln("notify: ", tname)
		msg.taskName = tname
		schedout <- msg.encode()
	}
	//wait for all tasks exit
exitloop:
	for m := range schedin {
		err := msg.decode(m)
		if err != nil {
			log.Infoln("failed decode msg: ", err)
			continue
		}
		switch {
		case msg.currFloor == -1 && msg.goalFloor == -1:
			log.Infoln("finished task: ", msg.taskName)
			delete(tasks, msg.taskName)
			if len(tasks) == 0 {
				break exitloop
			}
		default:
			log.Infof("recv %v\n", msg)
		}
	}
	//tell sched event loop exit
	exitCh <- true
	//wait for it
	<- exitCh
}

// ----------------------- func main() ------------------------- //

func main() {

	schedConfig := got.LoadSchedulerConfig()

	schd := got.NewGoTaskScheduler(
		"", //userName, Mesos-go will fill in user.
		schedConfig,
		nil)

	schd.SpawnTask("elevator", DefaultNumberOfElevators,
		map[string]float64{
			"cpus": 0.1,
			"mem":  96,
		},
	)
	schd.SpawnTask("floor", DefaultNumberOfFloors,
		map[string]float64{
			"cpus": 0.1,
			"mem":  96,
		},
	)
	schd.RegisterSchedFunc(schedFunc)

	driver, err := sched.NewMesosSchedulerDriver(schd.DriverConfig())

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}

}
