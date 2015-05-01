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
	"errors"
	"flag"
	"fmt"
	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"strings"
	"sync"
)

const (
	//Default buffer size of channels for communication between scheduler and tasks.
	DefTaskChanLen = 256
)

type appTaskStatus struct {
	Name    string
	Status  mesos.TaskState //int32: TaskState_TASK_RUNNING,...
	chanin  chan GoTaskMsg
	chanout chan GoTaskMsg
}

//The type of tasks.
//App tasks will use channel "in" to receive messages from schedulers.
//App tasks will send messages to scheduler via channel "out".
//channel "in" will be closed to notify task to exit.
type AppTask interface {
	Run(in <-chan GoTaskMsg, out chan<- GoTaskMsg, args []string /*, env map[string]string*/) error
}

//The type of task functions.
//App tasks will use channel "in" to receive messages from schedulers.
//App tasks will send messages to scheduler via channel "out".
//channel "in" will be closed to notify task to exit.
type AppTaskFunc func(in <-chan GoTaskMsg, out chan<- GoTaskMsg, args []string /*, env map[string]string*/) error

func (atf AppTaskFunc) Run(in <-chan GoTaskMsg, out chan<- GoTaskMsg, args []string /*, env map[string]string*/) error {
	return atf(in, out, args /*,env*/)
}

//Common interface of app task executor.
type AppTaskExecutor interface {
	//Start/dispatch app tasks based on task name.
	//App tasks will use channel "in" to receive messages from schedulers.
	//App tasks will send messages to scheduler via channel "out".
	RunTask(taskName string, in <-chan GoTaskMsg, out chan<- GoTaskMsg /*, args []string, env map[string]string*/) error
	//register a task to a task name
	RegisterTask(name string, task AppTask)
	//register a task function to a task name
	RegisterTaskFunc(name string, task AppTaskFunc)
}

//Default app task executor, used if no external appTaskExecutor specified when
//creating GoTaskExecutor
type DefAppTaskExecutor struct {
	mux     map[string]AppTask
	muxLock sync.Mutex
}

func NewDefAppTaskExecutor() *DefAppTaskExecutor {
	return &DefAppTaskExecutor{mux: make(map[string]AppTask)}
}

//Register a task to a task name in default app task executor
func (exec *DefAppTaskExecutor) RegisterTask(name string, task AppTask) {
	exec.muxLock.Lock()
	defer exec.muxLock.Unlock()
	exec.mux[name] = task
}

//Register a task func to a task name in default app task executor
func (exec *DefAppTaskExecutor) RegisterTaskFunc(name string, taskFunc AppTaskFunc) {
	exec.muxLock.Lock()
	defer exec.muxLock.Unlock()
	exec.mux[name] = AppTaskFunc(taskFunc)
}

//Dispatch task based on task name, already run in its own goroutine
func (ee *DefAppTaskExecutor) RunTask(taskName string, chanin <-chan GoTaskMsg, chanout chan<- GoTaskMsg /*, args []string, env map[string]string*/) error {
	pos := strings.LastIndex(taskName, "-")
	if pos > 0 {
		//taskName[:pos] is task type name, used for dispatch
		ee.muxLock.Lock()
		task := ee.mux[taskName[:pos]]
		ee.muxLock.Unlock()
		args := []string{taskName}
		return task.Run(chanin, chanout, args)
	}
	return errors.New("Cannot find handler")
}

//Responsible for starting app tasks and forwarding messages between scheduler and tasks.
type GoTaskExecutor struct {
	driver   exec.ExecutorDriver
	taskExec AppTaskExecutor
	appTasks map[string]*appTaskStatus
	//protect appTasks map, since Executor's mesos callbacks can be invoked from multi goroutines
	taskMapLock sync.Mutex
	fwMsgChan   chan GoTaskMsg
}

//Create a Go Task Executor to be used with Mesos Executor Driver.
//Use an AppTaskExecutor to do app specific dispatching to tasks.
func NewGoTaskExecutor(ae AppTaskExecutor) (exec *GoTaskExecutor) {
	exec = &GoTaskExecutor{
		driver:    nil,
		taskExec:  ae,
		appTasks:  make(map[string]*appTaskStatus),
		fwMsgChan: make(chan GoTaskMsg, 2*DefTaskChanLen),
	}
	if ae == nil {
		exec.taskExec = NewDefAppTaskExecutor()
	}
	return
}

//Register a task to a task name in go task executor
func (exec *GoTaskExecutor) RegisterTask(name string, task AppTask) {
	exec.taskExec.RegisterTask(name, task)
}

//Register a task func to a task name in go task executor
func (exec *GoTaskExecutor) RegisterTaskFunc(name string, taskFunc AppTaskFunc) {
	exec.taskExec.RegisterTaskFunc(name, taskFunc)
}

//Mesos framework method.
func (exec *GoTaskExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

//Mesos framework method.
func (exec *GoTaskExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

//when disconnected from slave, oure communication thru Framework message
//should be broken, tell tasks to exit
func (exec *GoTaskExecutor) Disconnected(driver exec.ExecutorDriver) {
	fmt.Println("Executor disconnected, tell all tasks exit.")
	exec.taskMapLock.Lock()
	defer exec.taskMapLock.Unlock()
	for k, v := range exec.appTasks {
		//nil tasks, so no further messages will be sent to it
		exec.appTasks[k] = nil
		//close task's input chan to tell it exit
		close(v.chanin)
	}
}

//Launch app task in a separate goroutine.
func (exec *GoTaskExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	exec.taskMapLock.Lock()
	defer exec.taskMapLock.Unlock()
	if exec.driver == nil {
		exec.driver = driver
		go exec.runMsgPump()
	}
	tname := taskInfo.GetName()
	if exec.appTasks[tname] != nil {
		fmt.Println("XX duplicated task name")
		return
	}
	fmt.Println("Launching task--", tname)
	appSt := &appTaskStatus{
		Name:    tname,
		Status:  mesos.TaskState_TASK_RUNNING,
		chanin:  make(chan GoTaskMsg, DefTaskChanLen),
		chanout: exec.fwMsgChan,
	}
	exec.appTasks[tname] = appSt
	fmt.Println("XXXTotal tasks launching ", len(exec.appTasks))
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

		//
		// this is where one would perform the requested task
		//
		err = exec.taskExec.RunTask(tname, chanin, chanout)

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
		//when all tasks exits, tell executor forward goroutine exit
		exec.taskMapLock.Lock()
		defer exec.taskMapLock.Unlock()
		delete(exec.appTasks, tname)
		if len(exec.appTasks) == 0 {
			close(exec.fwMsgChan)
		}
	}(tname, appSt.chanin, appSt.chanout)
}

//Scheduler kills a task, close it input chan.
func (exec *GoTaskExecutor) KillTask(driver exec.ExecutorDriver, taskId *mesos.TaskID) {
	taskName := taskId.GetValue()
	fmt.Println("Kill task: ", taskName)
	exec.taskMapLock.Lock()
	defer exec.taskMapLock.Unlock()
	task := exec.appTasks[taskName]
	//remove task, so it will not sent messages
	if task != nil {
		//set task to nil, so no more message forwarding
		exec.appTasks[taskName] = nil
		//close input chan, tell task exit
		close(task.chanin)
	}
}

//Forward messages from scheduler to tasks.
func (exec *GoTaskExecutor) FrameworkMessage(driver exec.ExecutorDriver, rawMsg string) {
	//fmt.Println("Got framework message: ", msg)
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		fmt.Println("failed to decode message target")
		return
	}
	exec.taskMapLock.Lock()
	task := exec.appTasks[msg.TaskName]
	exec.taskMapLock.Unlock()
	if task != nil {
		task.chanin <- msg
	}
}

//Scheduler shuts down, all tasks have to exit
func (exec *GoTaskExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Scheduler shuts down, all tasks have to exit")
	exec.taskMapLock.Lock()
	defer exec.taskMapLock.Unlock()
	for k, v := range exec.appTasks {
		exec.appTasks[k] = nil
		close(v.chanin)
	}
}

//Unrecoverable error, tell all tasks exit
func (exec *GoTaskExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Unrecoverable error, all tasks exit :", err)
	exec.taskMapLock.Lock()
	defer exec.taskMapLock.Unlock()
	for k, v := range exec.appTasks {
		exec.appTasks[k] = nil
		close(v.chanin)
	}
}

//Forward messages from tasks to scheduler.
func (exc *GoTaskExecutor) runMsgPump() {
	fmt.Println("start sending framework messages to scheduler")
	for msg := range exc.fwMsgChan {
		data, err := EncodeMsg(msg)
		if err != nil {
			fmt.Println("failed to encode msg: ", err)
			continue
		}
		_, err = exc.driver.SendFrameworkMessage(data)
		if err != nil {
			fmt.Println("failed SendFrameworkMessage: ", err)
		}
	}
	fmt.Println("stop sending framework messages to scheduler")
}

//Return Mesos driver config for this GoTask executor.
func (exc *GoTaskExecutor) DriverConfig() exec.DriverConfig {
	return exec.DriverConfig{
		Executor: exc,
	}
}

// -------------------------- func inits () ----------------- //
func init() {
	flag.Parse()
}
