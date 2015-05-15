mesosgot: Simple Go Task Scheduler on Mesos (prototype)
=======================================================

1. very thin layer over mesos-go api (and example scheduler/executor); with simple API (http://godoc.org/github.com/yglcode/mesosgot).

2. for launching cluster of relatively long running tasks running in its own goroutine at slave machines.

3. each task is a Go function with following signature which will automatically run in a goroutine:

    	func(in <-chan TaskMsg, out chan<-TaskMsg, args []string, env map[string]string) error

	* App tasks will use channel "in" to receive messages from schedulers.
	* App tasks will send messages to scheduler via channel "out".
	* Channel "in" will be closed to tell tasks to exit (such as when killed by scheduler, or system shuts down). 
      
4. application scheduler is also a go function automatically running in a goroutine:

    	func(schedin <-chan TaskMsg, schedout chan<-TaskMsg, schedevent <-chan SchedEvent)

	* App scheduler will use channel "schedin" to receive messages from tasks.
	* App scheduler will send messages to tasks via "schedout" channel.
	* App scheduler will receive scheduling events from "schedevent" channel.
      
5. scheduler & tasks communicate thru Go channels(in,out) overlaying on top of native framework communication api.

6. simple/static resource allocation:
	* only accept resource offers when resources required by all tasks are offered
	* whenever any task fail, whole system shuts down

7. programming:
	* build two separate executables:
		* app_scheduler: app scheduling logic to run at cluster master.
		* app_executor: containing all app tasks functions, their registration and dispatching logic to run at cluster slave machines.

	* app_scheduler: create GoTaskScheduler and plug into MesosSchedulerDriver

		* call GoTaskScheduler.SpawnTask() to launch named app tasks in cluster.
		* call GoTaskScheduler.RegisterSchedFunc() to register scheduler function.

	* app_executor: create GoTaskExecutor and plug into MesosExecutorDriver
      
		* call GoTaskExecutor.RegisterTask() to register app tasks to a name.
		* call GoTaskExecutor.RegisterTaskFunc() to register app task function to a name.

8. implement an elevator control system on top of it as example.

