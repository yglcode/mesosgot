// +build gotask-exec

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
	"fmt"
	exec "github.com/mesos/mesos-go/executor"
	got "github.com/yglcode/mesosgot"
)

func exampleTaskMain(chanin <-chan got.GoTaskMsg, chanout chan<- got.GoTaskMsg, args []string /*, env map[string]string*/) error {
	myName := args[0]
	//first report myself to scheduler
	msg := &schedMsg{myName, 0, 0}
	chanout <- msg.encode()
	//wait for scheduler and other tasks ready
	<-chanin
	//then send 3 msgs to scheduler
	for i := 0; i < 3; i++ {
		msg := &schedMsg{myName, i, 10 + i}
		chanout <- msg.encode()
	}
	//then tell scheduler i exit
	msg = &schedMsg{myName, -1, -1}
	chanout <- msg.encode()
	return nil
}

func main() {
	fmt.Println("Starting Elevator Executor")

	exc := got.NewGoTaskExecutor(nil)
	exc.RegisterTaskFunc("exampleTask1", exampleTaskMain)
	exc.RegisterTaskFunc("exampleTask2", exampleTaskMain)

	driver, err := exec.NewMesosExecutorDriver(exc.DriverConfig())

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}
