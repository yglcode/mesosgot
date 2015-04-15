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
	got "github.com/yglcode/mesosgot"
)

type schedMsg struct {
	taskName  string
	currFloor int
	goalFloor int
}

func (tm *schedMsg) encode() (res got.GoTaskMsg) {
	res.TaskName = tm.taskName
	res.MessageData = fmt.Sprintf("%d %d", tm.currFloor, tm.goalFloor)
	return
}

func (tm *schedMsg) decode(msg got.GoTaskMsg) (err error) {
	tm.taskName = msg.TaskName
	_, err = fmt.Sscanf(msg.MessageData, "%d %d", &tm.currFloor, &tm.goalFloor)
	return
}
