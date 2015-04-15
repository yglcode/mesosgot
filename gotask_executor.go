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
package mesosgot

import (
	"flag"
	"fmt"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

const (
	DefTaskChanLen = 256
)

type AppTaskStatus struct {
	Name string
	Status mesos.TaskState //int32: TaskState_TASK_RUNNING,...
	chanin chan GoTaskMsg
	chanout chan GoTaskMsg
}

type AppTaskFunc func(in <- chan GoTaskMsg, out chan<-GoTaskMsg, args []string/*, env map[string]string*/) error

type AppTaskExecutor interface {
	RunTask(taskName string, in <- chan GoTaskMsg, out chan<-GoTaskMsg/*, args []string, env map[string]string*/) error
}

type GoTaskExecutor struct {
	tasksLaunched int
	driver exec.ExecutorDriver
	appExec AppTaskExecutor
	appTasks map[string]*AppTaskStatus
	fwMsgChan chan GoTaskMsg
	exitChan chan struct{}
}

func NewGoTaskExecutor(ae AppTaskExecutor) (exec *GoTaskExecutor) {
	exec = &GoTaskExecutor{
		tasksLaunched: 0,
		driver:nil,
		appExec: ae,
		appTasks: make(map[string]*AppTaskStatus),
		fwMsgChan: make(chan GoTaskMsg, 2*DefTaskChanLen),
		exitChan: make(chan struct{}),
	}
	return
}

func (exec *GoTaskExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *GoTaskExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *GoTaskExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *GoTaskExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	if exec.driver == nil {
		exec.driver = driver
		go exec.RunMsgPump()
	}
	tname := taskInfo.GetName()
	if exec.appTasks[tname] != nil {
		fmt.Println("XX duplicated task name")
		return
	}
	appSt := &AppTaskStatus{
		Name: tname,
		Status: mesos.TaskState_TASK_RUNNING,
		chanin: make(chan GoTaskMsg, DefTaskChanLen),
		chanout: exec.fwMsgChan,
	}
	exec.appTasks[tname] = appSt
	exec.tasksLaunched++
	go func(tname string, chanin chan GoTaskMsg, chanout chan GoTaskMsg) {
		fmt.Println("Launching task", tname, "with command", taskInfo.Command.GetValue())

		runStatus := &mesos.TaskStatus{
			TaskId: taskInfo.GetTaskId(),
			State:  mesos.TaskState_TASK_RUNNING.Enum(),
		}
		_, err := driver.SendStatusUpdate(runStatus)
		if err != nil {
			fmt.Println("Got error", err)
		}

		fmt.Println("XXXTotal tasks launched ", exec.tasksLaunched)
		//
		// this is where one would perform the requested task
		//
		err = exec.appExec.RunTask(tname, chanin, chanout)

		if err != nil {
			// finish task
			fmt.Println("Error task", taskInfo.GetName())
			finStatus := &mesos.TaskStatus{
				TaskId: taskInfo.GetTaskId(),
				State:  mesos.TaskState_TASK_FAILED.Enum(),
			}
			_, err = driver.SendStatusUpdate(finStatus)
			if err != nil {
				fmt.Println("Got error", err)
			}
		} else {
			// finish task
			fmt.Println("Finishing task", taskInfo.GetName())
			finStatus := &mesos.TaskStatus{
				TaskId: taskInfo.GetTaskId(),
				State:  mesos.TaskState_TASK_FINISHED.Enum(),
			}
			_, err = driver.SendStatusUpdate(finStatus)
			if err != nil {
				fmt.Println("Got error", err)
			}
			fmt.Println("Task finished", taskInfo.GetName())
		}
	}(tname, appSt.chanin, appSt.chanout)
}

func (exec *GoTaskExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *GoTaskExecutor) FrameworkMessage(driver exec.ExecutorDriver, rawMsg string) {
	//fmt.Println("Got framework message: ", msg)
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		fmt.Println("failed to decode message target")
		return
	}
	exec.appTasks[msg.TaskName].chanin <- msg
}

func (exec *GoTaskExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
	close(exec.exitChan) //shutdown framework msg pump
}

func (exec *GoTaskExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func (exc *GoTaskExecutor) RunMsgPump() {
	fmt.Println("start sending framework messages to scheduler")
	msgpump: for {
		select {
		case msg := <- exc.fwMsgChan:
			data, err := EncodeMsg(msg)
			if err != nil {
				fmt.Println("failed to encode msg: ", err)
				continue msgpump
			}
			_, err = exc.driver.SendFrameworkMessage(data)
			if err != nil {
				fmt.Println("failed SendFrameworkMessage: ",err)
			}
		case <-exc.exitChan:
			break msgpump
		}
	}
	fmt.Println("stop sending framework messages to scheduler")
}

func (exc *GoTaskExecutor) DriverConfig() exec.DriverConfig {
	return exec.DriverConfig{
		Executor: exc,
	}
}

// -------------------------- func inits () ----------------- //
func init() {
	flag.Parse()
}
