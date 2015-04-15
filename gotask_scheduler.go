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
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"bytes"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"golang.org/x/net/context"
)

//GoTask scheduler/framework config parameters
type GoTaskSchedConfig struct {
	Address string
	ArtifactPort int
	AuthProvider string
	Master string
	ExecutorPath string
	MesosAuthPrincipal string
	MesosAuthSecretFile string
	TaskRefuseSeconds float64
}

//standard command line flags for specifying GoTask scheduler config
var (
	address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", 12345, "Binding port for artifact server")
	authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	executorPath        = flag.String("executor", "./example_executor", "Path to app framework executor")
	mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")
)

func init() {
	flag.Parse()
}

func LoadSchedulerConfig() (config *GoTaskSchedConfig) {
	log.Infoln("Loading GoTask Scheduler Configurations ...")
	config = &GoTaskSchedConfig {
		Address: *address,
		ArtifactPort: *artifactPort,
		AuthProvider: *authProvider,
		Master: *master,
		ExecutorPath: *executorPath,
		MesosAuthPrincipal: *mesosAuthPrincipal,
		MesosAuthSecretFile: *mesosAuthSecretFile,
		TaskRefuseSeconds: 60,
	}
	return
}

//--- GoTask scheduler -----
type GoTaskScheduler struct {
	config *GoTaskSchedConfig
	executor      *mesos.ExecutorInfo
	driver sched.SchedulerDriver
	fwinfo *mesos.FrameworkInfo
	cred *mesos.Credential
	bindAddr net.IP
	user string
	name string
	appSched AppTaskScheduler
	tasks map[string]*mesos.TaskInfo
	schedin chan GoTaskMsg
	schedout chan GoTaskMsg
	schedevent chan SchedEvent
	exitChan chan struct{}
	cpuSize float64
	memSize float64
	tasksLaunched int
	tasksRunning int
	tasksFinished int
	totalTasks    int
}

//placeholder, add detailed event tags
type SchedEvent struct {
	TaskName string
	Message string
}

type AppTaskResourceInfo struct {
	Name string
	Count int
	CpusPerTask float64
	MemPerTask float64
}

//common interface implmented by all app/framework scheduler
type AppTaskScheduler interface {
	//return resource requirements of all tasks
	TasksResourceInfo() []*AppTaskResourceInfo
	//start running scheduler
	RunScheduler(schedin <-chan GoTaskMsg, schedout chan<-GoTaskMsg, schedevent chan<-SchedEvent)
}

func NewGoTaskScheduler(userName string, aps AppTaskScheduler, conf *GoTaskSchedConfig) (sched *GoTaskScheduler) {
	sched = &GoTaskScheduler{
		config: conf,
		executor: nil,
		driver: nil,
		fwinfo: nil,
		cred: nil,
		bindAddr: nil,
		user: userName, 
		name: "GoTask Framework",
		appSched: aps,
		tasks: make(map[string]*mesos.TaskInfo),
		schedin: make(chan GoTaskMsg, 2*DefTaskChanLen),
		schedout: make(chan GoTaskMsg, 2*DefTaskChanLen),
		schedevent: make(chan SchedEvent, 2*DefTaskChanLen),
		exitChan: make(chan struct{}),
		cpuSize: 0,
		memSize: 0,
		tasksLaunched: 0,
		tasksRunning: 0,
		tasksFinished: 0,
		totalTasks:    0,
	}
	//calc appTasks resource requirements
	taskResInfo := aps.TasksResourceInfo()
	for _, tri := range taskResInfo {
		sched.totalTasks+=tri.Count
		sched.cpuSize+=float64(tri.Count)*tri.CpusPerTask
		sched.memSize+=float64(tri.Count)*tri.MemPerTask
	}
	// build command executor
	sched.executor = prepareExecutorInfo(conf)
	//
	sched.fwinfo = &mesos.FrameworkInfo{
		User: proto.String(sched.user), 
		Name: proto.String(sched.name),
	}

	sched.cred = (*mesos.Credential)(nil)
	if conf.MesosAuthPrincipal != "" {
		sched.fwinfo.Principal = proto.String(conf.MesosAuthPrincipal)
		secret, err := ioutil.ReadFile(conf.MesosAuthSecretFile)
		if err != nil {
			log.Fatal(err)
		}
		sched.cred = &mesos.Credential{
			Principal: proto.String(conf.MesosAuthPrincipal),
			Secret:    secret,
		}
	}

	addr, err := net.LookupIP(conf.Address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", conf.Address)
	}
	sched.bindAddr = addr[0]

	return
}

func (sched *GoTaskScheduler) RunMsgPump() {
	fmt.Println("start sending framework messages to tasks")
	msgpump: for {
		select {
		case msg := <- sched.schedout:
			taskInfo := sched.tasks[msg.TaskName]
			data, err := EncodeMsg(msg)
			if err != nil {
				log.Infoln("failed to encode msg: ",err)
				continue msgpump
			}
			_, err = sched.driver.SendFrameworkMessage(sched.executor.ExecutorId, taskInfo.SlaveId, data)
			if err != nil {
				log.Infoln("failed SendFrameworkMessage to tasks: ",err)
			}
		case <-sched.exitChan:
			break msgpump
		}
	}
	fmt.Println("stop sending framework messages to tasks")
}

func (sched *GoTaskScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}

func (sched *GoTaskScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}

func (sched *GoTaskScheduler) Disconnected(sched.SchedulerDriver) {}

func (sched *GoTaskScheduler) haveEnoughResources(offers []*mesos.Offer) bool {
	cpus := 0.0
	mems := 0.0
	for _, offer := range offers {
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}
	}
	if cpus >= sched.cpuSize && mems >= sched.memSize {
		return true
	}
	return false
}

func (sched *GoTaskScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	//if already launched, or not enough resource, return
	if sched.tasksLaunched > 0 || !sched.haveEnoughResources(offers) {
		return
	}
	if sched.driver == nil {
		sched.driver = driver
		//start msg pump, it will exit by exitChan
		go sched.RunMsgPump()
		//start app/framework scheduler
		go func() {
			sched.appSched.RunScheduler(sched.schedin, sched.schedout, sched.schedevent)
			//scheduler exit, stop all
			sched.driver.Stop(false)
		}()
	}

	taskCnt := 0
	taskIdx := 0
	tasksResInfo := sched.appSched.TasksResourceInfo()
	resInfo := tasksResInfo[taskIdx]
	for _, offer := range offers {
		if sched.tasksLaunched >= sched.totalTasks {
			break
		}
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo

		for sched.tasksLaunched < sched.totalTasks &&
			resInfo.CpusPerTask <= remainingCpus &&
			resInfo.MemPerTask <= remainingMems {

			sched.tasksLaunched++

			taskIdStr := strconv.Itoa(taskCnt)
			name := resInfo.Name + "-" + taskIdStr
			taskId := &mesos.TaskID{
				Value: proto.String(name),
			}
			task := &mesos.TaskInfo{
				Name:     proto.String(name),
				TaskId:   taskId, //make it same as Name, since StatusUpdate() only provides TaskId, not name
				SlaveId:  offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", resInfo.CpusPerTask),
					util.NewScalarResource("mem", resInfo.MemPerTask),
				},
			}
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			sched.tasks[name] = task
			tasks = append(tasks, task)
			remainingCpus -= resInfo.CpusPerTask
			remainingMems -= resInfo.MemPerTask
			//
			taskCnt++
			if taskCnt >= resInfo.Count {
				taskIdx++
				if taskIdx >= len(tasksResInfo) {
					break
				}
				resInfo = tasksResInfo[taskIdx]
				taskCnt = 0
			}
		}
		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(sched.config.TaskRefuseSeconds)})
	}
}

func (sched *GoTaskScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	taskId := status.TaskId.GetValue()
	log.Infoln("Status update: task ", taskId, " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}
	if status.GetState() == mesos.TaskState_TASK_RUNNING {
		sched.tasksRunning++
	}

	//if sched.tasksFinished >= sched.totalTasks {
	if sched.tasksFinished >= sched.tasksRunning {
		log.Infoln("All running tasks completed.")
		/* don't stop framework from here, wait till app scheduler exit
		log.Infoln("All running tasks completed, stopping framework.")
		close(sched.exitChan) //stop msgpump
		driver.Stop(false)
                */
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		close(sched.exitChan) //stop msgpump
		driver.Abort()
	}
}

func (sched *GoTaskScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *GoTaskScheduler) FrameworkMessage(driver sched.SchedulerDriver, execid *mesos.ExecutorID, slaveid *mesos.SlaveID, rawMsg string) {
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		log.Infoln("failed to decode msg: ",err)
	} else {
		sched.schedin <-msg
	}
}
func (sched *GoTaskScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}
func (sched *GoTaskScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (sched *GoTaskScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}

func (schd *GoTaskScheduler) DriverConfig() (drvConfig sched.DriverConfig) {
	drvConfig = sched.DriverConfig{
		Scheduler:      schd,
		Framework:      schd.fwinfo,
		Master:         schd.config.Master,
		Credential:     schd.cred,
		BindingAddress: schd.bindAddr,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, schd.config.AuthProvider)
			ctx = sasl.WithBindingAddress(ctx, schd.bindAddr)
			return ctx
		},
	}
	return
}

// returns (downloadURI, basename(path))
func serveExecutorArtifact(config *GoTaskSchedConfig) (*string, string) {
	serveFile := func(pattern string, filename string) {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filename)
		})
	}

	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(config.ExecutorPath, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = config.ExecutorPath
	}
	serveFile("/"+base, config.ExecutorPath)

	hostURI := fmt.Sprintf("http://%s:%d/%s", config.Address, config.ArtifactPort, base)
	log.V(2).Infof("Hosting artifact '%s' at '%s'", config.ExecutorPath, hostURI)

	return &hostURI, base
}

func prepareExecutorInfo(config *GoTaskSchedConfig) *mesos.ExecutorInfo {
	executorUris := []*mesos.CommandInfo_URI{}
	uri, executorCmd := serveExecutorArtifact(config)
	executorUris = append(executorUris, &mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})

	executorCommand := fmt.Sprintf("./%s", executorCmd)

	go http.ListenAndServe(fmt.Sprintf("%s:%d", config.Address, config.ArtifactPort), nil)
	log.V(2).Info("Serving executor artifacts...")

	// Create mesos scheduler driver.
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
			Uris:  executorUris,
		},
	}
}

//TaskMsg and encoder/decoder
//GoTask_framework only has scheduler<->tasks communications, so simple rule:
//msg.TaskName always points to task's name, no matter which direction it goes,
//so it can be either msg source or msg destination.
type GoTaskMsg struct {
	TaskName string
	MessageData string
}

//simple encoding/decoding using a delimiter '^', assuming no taskName use it
const (
	msg_field_delimiter = '^'
)

func EncodeMsg(m GoTaskMsg) (s string, e error) {
	var buf bytes.Buffer
	_, e = buf.WriteString(m.TaskName)
	if e!=nil {
		return
	}
	e = buf.WriteByte(msg_field_delimiter)
	if e!= nil {
		return
	}
	_, e = buf.WriteString(m.MessageData)
	if e != nil {
		return
	}
	s = buf.String()
	return
}

func DecodeMsg(s string) (m GoTaskMsg, e error) {
	buf := bytes.NewBufferString(s)
	m.TaskName, e = buf.ReadString(msg_field_delimiter)
	if e != nil {
		return
	}
	m.TaskName = m.TaskName[:len(m.TaskName)-1] //remove delimiter
	m.MessageData = buf.String()
	return
}
