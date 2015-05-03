/*
mesosgot: Simple Go Task Scheduler on Mesos (prototype)

1. very thin layer over mesos-go api (and example scheduler/executor).

2. for launching cluster of relatively long running tasks running in its own goroutine at slave machines.

3. each task is a Go function with following signature which will automatically run in a goroutine:
      func(in <-chan TaskMsg, out chan<-TaskMsg, args []string, env map[string]string) error
      App tasks will use channel "in" to receive messages from schedulers.
      App tasks will send messages to scheduler via channel "out".
      Channel "in" will be closed to tell task exit (such as when killed by scheduler, or system shuts down).

4. application scheduler is also a go function automatically running in a goroutine:
      func(schedin <-chan TaskMsg, schedout chan<-TaskMsg, schedevent <-chan SchedEvent)
      App scheduler will use channel "schedin" to receive messages from tasks.
      App scheduler will send messages to tasks via "schedout" channel.
      App scheduler will receive scheduling events from "schedevent" channel.

5. scheduler & tasks communicate thru Go channels(in,out) overlaying on top of native framework communication api.

6. simple/static resource allocation:
          * only accept resource offers when resources required by all tasks are offered
          * whenever any task fail, whole system shuts down

7. programming:
      * build two separate executables:
              * app_scheduler: app scheduling logic to run at cluster master.
              * app_executor: containing all app tasks functions, their registration and dispatching logic to run at cluster slave machines.

      * app_scheduler: create GoTaskScheduler and plug into MesosSchedulerDriver
            * call GoTaskScheduler.SpawnTask() to launch named tasks in cluster
            * call GoTaskScheduler.RegisterSchedFunc() to register scheduler function.

      * app_executor: create GoTaskExecutor and plug into MesosExecutorDriver
            * call GoTaskExecutor.RegisterTask() to register app task to a name.
            * call GoTaskExecutor.RegisterTaskFunc() to register app task function to a name.

Licensed under Apache 2.0
*/
package mesosgot

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"

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
	//Binding address for artifact server
	Address string
	//Binding port for artifact server
	ArtifactPort int
	//Authentication provider
	AuthProvider string
	//Master address, default 127.0.0.1:5050
	Master string
	//Path to app framework executor
	ExecutorPath string
	//Mesos authentication principal
	MesosAuthPrincipal string
	//Mesos authentication secret file
	MesosAuthSecretFile string
	TaskRefuseSeconds   float64
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

//Load scheduler config parameters from default settings and command line flags.
func LoadSchedulerConfig() (config *GoTaskSchedConfig) {
	log.Infoln("Loading GoTask Scheduler Configurations ...")
	config = &GoTaskSchedConfig{
		Address:             *address,
		ArtifactPort:        *artifactPort,
		AuthProvider:        *authProvider,
		Master:              *master,
		ExecutorPath:        *executorPath,
		MesosAuthPrincipal:  *mesosAuthPrincipal,
		MesosAuthSecretFile: *mesosAuthSecretFile,
		TaskRefuseSeconds:   60,
	}
	return
}

//GoTask scheduler, responsible for starting tasks at slaves, forwarding msgs between scheduler and tasks.
type GoTaskScheduler struct {
	config        *GoTaskSchedConfig
	executor      *mesos.ExecutorInfo
	driver        sched.SchedulerDriver
	fwinfo        *mesos.FrameworkInfo
	cred          *mesos.Credential
	bindAddr      net.IP
	user          string
	name          string
	appSched      AppTaskScheduler
	tasks         map[string]*mesos.TaskInfo
	schedin       chan GoTaskMsg
	schedout      chan GoTaskMsg
	schedevent    chan SchedEvent
	cpuSize       float64
	memSize       float64
	tasksLaunched int
	tasksRunning  int
	tasksFinished int
	totalTasks    int
}

//Scheduler events types
type SchedEventType int

const (
	Registered SchedEventType = iota
	Reregistered
	Disconnected
	TaskLaunched
	TaskRunning
	TaskFinished
	TaskFailed
	TaskKilled
	TaskLost
	OfferRescinded
	SlaveLost
	ExecutorLost
	Shutdown
	Error
	NumSchedEventTypes
)

var eventTypeNames = []string{
	"Registered",
	"Reregistered",
	"Disconnected",
	"TaskLaunched",
	"TaskRunning",
	"TaskFinished",
	"TaskFailed",
	"TaskKilled",
	"TaskLost",
	"OfferRescinded",
	"SlaveLost",
	"ExecutorLost",
	"Shutdown",
	"Error",
}

func (et SchedEventType) String() string {
	if et < NumSchedEventTypes {
		return eventTypeNames[et]
	}
	return "UnknownEventType"
}

//Scheduler events (such as task failure, disconnect, etc.) to be forwarded to App scheduler, and limited set of events to tasks (such as disconn, shutdown)
type SchedEvent struct {
	EventType SchedEventType
	EventData interface{} //the type of event data depend on event type
}

//AppTaskResourceInfo will allow app scheduler specify the resource requirements of app tasks.
type AppTaskResourceInfo struct {
	Name      string
	Count     int
	Resources map[string]float64 //map keys = Mesos resource names: "cpus", "mem"...
	Args      []string
	Env       map[string]string
}

//SchedulerTask for scheduling logic to run at cluster master.
type AppSchedulerTask interface {
	//App scheduler will use channel "schedin" to receive messages from tasks.
	//App scheduler will send messages to tasks via "schedout" channel.
	//App scheduler will receive scheduling events from "schedevent" channel.
	Run(schedin <-chan GoTaskMsg, schedout chan<- GoTaskMsg, schedevent <-chan SchedEvent)
}

//SchedulerFunc for scheduling logic to run at cluster master.
//App scheduler will use channel "schedin" to receive messages from tasks.
//App scheduler will send messages to tasks via "schedout" channel.
//App scheduler will receive scheduling events from "schedevent" channel.
type AppSchedulerFunc func(schedin <-chan GoTaskMsg, schedout chan<- GoTaskMsg, schedevent <-chan SchedEvent)

func (asf AppSchedulerFunc) Run(schedin <-chan GoTaskMsg, schedout chan<- GoTaskMsg, schedevent <-chan SchedEvent) {
	asf(schedin, schedout, schedevent)
}

//Common interface implmented by all app/framework scheduler
type AppTaskScheduler interface {
	//-- internal interface to mesos --
	//return resource requirements of all tasks
	TasksResourceInfo() []*AppTaskResourceInfo
	//start running app scheduler in a separate goroutine.
	//App scheduler will use channel "schedin" to receive messages from tasks.
	//App scheduler will send messages to tasks via "schedout" channel.
	//App scheduler will receive scheduling events from "schedevent" channel.
	RunScheduler(schedin <-chan GoTaskMsg, schedout chan<- GoTaskMsg, schedevent <-chan SchedEvent)

	//-- external interface to user code --
	//add one app task resource info
	SpawnTask(name string, count int, res map[string]float64, /*args, env*/)
	//add a set of app tasks
	SpawnTasks(tasks []*AppTaskResourceInfo)
	//register scheduler task
	RegisterSchedTask(sched AppSchedulerTask)
	//register scheduler func
	RegisterSchedFunc(sched AppSchedulerFunc)
}

//when no external AppTaskScheduler is provided when GoTaskScheduler is created, a default DefAppTaskScheduler is used
type DefAppTaskScheduler struct {
	tasksResourceReqs []*AppTaskResourceInfo
	appSched AppSchedulerTask
}

//create a default AppTaskScheduler
func NewDefAppTaskScheduler() *DefAppTaskScheduler {
	return &DefAppTaskScheduler{}
}

//return resource requests by app tasks
func (ats *DefAppTaskScheduler) TasksResourceInfo() []*AppTaskResourceInfo {
	return ats.tasksResourceReqs
}

//run registered app scheduler task/func in a goroutine
func (ats *DefAppTaskScheduler)	RunScheduler(schedin <-chan GoTaskMsg, schedout chan<- GoTaskMsg, schedevent <-chan SchedEvent) {
	if ats.appSched != nil {
		ats.appSched.Run(schedin, schedout, schedevent)
	}
}

//launch a app task in cluster
func (ats *DefAppTaskScheduler)	SpawnTask(name string, count int, res map[string]float64, /*args, env*/) {
	ats.tasksResourceReqs = append(ats.tasksResourceReqs,
		&AppTaskResourceInfo{
			Name: name,
			Count: count,
			Resources: res,
		})
}

//launch a set of app tasks in cluster
func (ats *DefAppTaskScheduler)	SpawnTasks(tasks []*AppTaskResourceInfo) {
	ats.tasksResourceReqs = append(ats.tasksResourceReqs, tasks...)
}

//register a app scheduler task to run in a goroutine
func (ats *DefAppTaskScheduler)	RegisterSchedTask(sched AppSchedulerTask) {
	ats.appSched = sched
}

//register a app scheduler func to run in a goroutine
func (ats *DefAppTaskScheduler)	RegisterSchedFunc(sched AppSchedulerFunc) {
	ats.appSched = AppSchedulerFunc(sched)
}

//Create a new Go Task Scheduler to be used with Mesos Scheduler Driver.
//If an instance of AppTaskScheduler is provided, it is used for tasks resource requirement and app scheduling; otherwise a default app task scheduler is used.
func NewGoTaskScheduler(userName string, conf *GoTaskSchedConfig, aps AppTaskScheduler) (sched *GoTaskScheduler) {
	sched = &GoTaskScheduler{
		config:        conf,
		executor:      nil,
		driver:        nil,
		fwinfo:        nil,
		cred:          nil,
		bindAddr:      nil,
		user:          userName,
		name:          "GoTask Framework",
		appSched:      aps,
		tasks:         make(map[string]*mesos.TaskInfo),
		schedin:       make(chan GoTaskMsg, 2*DefTaskChanLen),
		schedout:      make(chan GoTaskMsg, 2*DefTaskChanLen),
		schedevent:    make(chan SchedEvent, 2*DefTaskChanLen),
		cpuSize:       0,
		memSize:       0,
		tasksLaunched: 0,
		tasksRunning:  0,
		tasksFinished: 0,
		totalTasks:    0,
	}
	if aps == nil {
		sched.appSched = NewDefAppTaskScheduler()
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

//launch a app task in cluster
func (sched *GoTaskScheduler) SpawnTask(name string, count int, res map[string]float64, /*args, env*/) {
	sched.appSched.SpawnTask(name, count, res)
}

//launch a set of app tasks in cluster
func (sched *GoTaskScheduler) SpawnTasks(tasks []*AppTaskResourceInfo) {
	sched.appSched.SpawnTasks(tasks)
}

//register a app scheduler task to run in a goroutine
func (sched *GoTaskScheduler) RegisterSchedTask(schedT AppSchedulerTask) {
	sched.appSched.RegisterSchedTask(schedT)
}

//register a app scheduler func to run in a goroutine
func (sched *GoTaskScheduler) RegisterSchedFunc(schedF AppSchedulerFunc) {
	sched.appSched.RegisterSchedFunc(schedF)
}

//Responsible for forwarding msgs from scheduler to tasks at slave nodes.
func (sched *GoTaskScheduler) runMsgPump() {
	fmt.Println("start sending framework messages to tasks")
	for msg := range sched.schedout {
		taskInfo := sched.tasks[msg.TaskName]
		data, err := EncodeMsg(msg)
		if err != nil {
			log.Infoln("failed to encode msg: ", err)
			continue
		}
		_, err = sched.driver.SendFrameworkMessage(sched.executor.ExecutorId, taskInfo.SlaveId, data)
		if err != nil {
			log.Infoln("failed SendFrameworkMessage to tasks: ", err)
		}
	}
	fmt.Println("stop sending framework messages to tasks")
}

//Mesos framework method.
func (sched *GoTaskScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
	sched.calcResourceTotals()
}

//Mesos framework method.
func (sched *GoTaskScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
	sched.calcResourceTotals()
}

//Mesos framework method.
func (sched *GoTaskScheduler) Disconnected(driver sched.SchedulerDriver) {
	log.Infoln("Framework Disconnected with Driver")
	sched.schedevent <- SchedEvent{Disconnected, driver}
}

func (sched *GoTaskScheduler) calcResourceTotals() {
	//calc appTasks resource requirements
	taskResInfo := sched.appSched.TasksResourceInfo()
	for _, tri := range taskResInfo {
		sched.totalTasks += tri.Count
		sched.cpuSize += float64(tri.Count) * tri.Resources["cpus"]
		sched.memSize += float64(tri.Count) * tri.Resources["mem"]
	}
}

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

//Mesos framework method. Check resources available and start tasks at slave nodes.
func (sched *GoTaskScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	//if already launched, or not enough resource, return
	if sched.tasksLaunched > 0 || !sched.haveEnoughResources(offers) {
		return
	}
	if sched.driver == nil {
		sched.driver = driver
		//start msg pump, it will exit by exitChan
		go sched.runMsgPump()
		//start app/framework scheduler
		go func() {
			sched.appSched.RunScheduler(sched.schedin, sched.schedout, sched.schedevent)
			close(sched.schedout) //stop msgpump
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
			resInfo.Resources["cpus"] <= remainingCpus &&
			resInfo.Resources["mem"] <= remainingMems {

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
					util.NewScalarResource("cpus", resInfo.Resources["cpus"]),
					util.NewScalarResource("mem", resInfo.Resources["mem"]),
				},
			}
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			sched.tasks[name] = task
			tasks = append(tasks, task)
			remainingCpus -= resInfo.Resources["cpus"]
			remainingMems -= resInfo.Resources["mem"]
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
		sched.schedevent <- SchedEvent{TaskLaunched, len(tasks)}
	}
}

//Mesos framework method.
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
		driver.Stop(false)
		*/
	}

	switch {
	case status.GetState() == mesos.TaskState_TASK_LOST:
		sched.schedevent <- SchedEvent{TaskLost, status.TaskId}
	case status.GetState() == mesos.TaskState_TASK_KILLED:
		sched.schedevent <- SchedEvent{TaskKilled, status.TaskId}
	case status.GetState() == mesos.TaskState_TASK_FAILED:
		sched.schedevent <- SchedEvent{TaskFailed, status.TaskId}
	}
	/* allow scheduler to exit and shutdown from there
	log.Infoln(
		"Aborting because task", status.TaskId.GetValue(),
		"is in unexpected state", status.State.String(),
		"with message", status.GetMessage(),
	)
		driver.Abort()*/
}

//Mesos framework method.
func (sched *GoTaskScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

//Forward messages from tasks at slave nodes to scheduler.
func (sched *GoTaskScheduler) FrameworkMessage(driver sched.SchedulerDriver, execid *mesos.ExecutorID, slaveid *mesos.SlaveID, rawMsg string) {
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		log.Infoln("failed to decode msg: ", err)
	} else {
		sched.schedin <- msg
	}
}

//Mesos framework method.
func (sched *GoTaskScheduler) SlaveLost(driver sched.SchedulerDriver, slaveId *mesos.SlaveID) {
	sched.schedevent <- SchedEvent{SlaveLost, slaveId}
}

//Mesos framework method.
func (sched *GoTaskScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

//Mesos framework method.
func (sched *GoTaskScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
	sched.schedevent <- SchedEvent{Error, err}
}

//Return a Mesos scheduler driver for this scheduler.
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

//Returns (downloadURI, basename(path))
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

	executorCommand := fmt.Sprintf("./%s -logtostderr=true -v=3 --decode-routines=2", executorCmd)

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

/*TaskMsg and encoder/decoder.
GoTask_framework only has scheduler<->tasks communications, so simple rule:
msg.TaskName always points to task's name, no matter which direction it goes,
so it can be either msg source or msg destination.
*/
type GoTaskMsg struct {
	TaskName    string
	MessageData string
}

//simple encoding/decoding using a delimiter '^', assuming no taskName use it
const (
	msg_field_delimiter = '^'
)

//Encode GoTaskMsg to a string to be passed thru Mesos native FrameworkMessage.
func EncodeMsg(m GoTaskMsg) (s string, e error) {
	var buf bytes.Buffer
	_, e = buf.WriteString(m.TaskName)
	if e != nil {
		return
	}
	e = buf.WriteByte(msg_field_delimiter)
	if e != nil {
		return
	}
	_, e = buf.WriteString(m.MessageData)
	if e != nil {
		return
	}
	s = buf.String()
	return
}

//Decode a string (from Mesos native FrameworkMessage) to GoTaskMsg to be passed to tasks and scheduler.
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
