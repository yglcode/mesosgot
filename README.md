mesosgot: Simple Go Task Scheduler on Mesos (prototype)
=======================================================

1. very thin layer over mesos-go api (and example scheduler/executor).

2. for launching cluster of relatively long running tasks running in its own goroutine at slave machines.

3. each task is a Go function with following signature which will automatically run in a goroutine:

    	func(in <-chan TaskMsg, out chan<-TaskMsg, args []string, env map[string]string) error

	* App tasks will use channel "in" to receive messages from schedulers.
	* App tasks will send messages to scheduler via channel "out".
	* Channel "in" will be closed to tell tasks to exit (such as when killed by scheduler, or system shuts down). 
      
4. application scheduler is also a go function automatically running in a goroutine:

    	RunScheduler(schedin <-chan TaskMsg, schedout chan<-TaskMsg, schedevent <-chan SchedEvent)

	* App scheduler will use channel "schedin" to receive messages from tasks.
	* App scheduler will send messages to tasks via "schedout" channel.
	* App scheduler will receive scheduling events from "schedevent" channel.
      
5. scheduler & tasks communicate thru Go channels(in,out) overlaying on top of native framework communication api.

6. simple/static resource allocation:
	* only accept resource offers when resources required by all tasks are offered
	* whenever any task fail, whole system shuts down

7. programming:
	* build two separate executables:
		* app_scheduler: app scheduling logic
		* app_executor: containing all app tasks functions, their registration and dispatching

	* app_scheduler: create GoTaskScheduler and plug into MesosSchedulerDriver

		* GoTaskScheduler will need a AppTaskScheduler as following:

    			type AppTaskScheduler interface {
    				//return resource requirements of all tasks
    				TasksResourceInfo() []*AppTaskResourceInfo
    				//start running app scheduler
    				RunScheduler(schedin <-chan TaskMsg, schedout chan<-TaskMsg, schedevent <-chan SchedEvent)
    			}

		* App scheduling logic is defined inside RunSchededuler().

	* app_executor: create GoTaskExecutor and plug into MesosExecutorDriver
      
		* GoTaskExecutor will need a AppTaskExecutor as following:

    			type AppTaskExecutor interface {
    				RunTask(taskName string, in <- chan TaskMsg, out chan<-TaskMsg/*, args []string, env map[string]string*/) error
    			}

		* Inside RunTask(), call is dispatched by taskName and proper registered task function is called.

8. implement an elevator control system on top of it as example.

