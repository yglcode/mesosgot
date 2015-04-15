// Elevator scheduling system emulation:
// two design considerations:
// 1. use goroutines and channels to model system components and their communication:
//    . Components:
//         Elevator,Floor and Scheduler each run in its own goroutine:
//    . Communication:
//       . pickupReqChan: Floors send PickUp requests to Scheduler
//       . schedChan: Scheduler sends scheduling commands to Elevator
//       . statusChan: Elevators report status (currFloor, goalFloor) to Scheduler
// 2. Scheduling algorithm:
//    keep each elevator runing till end and then reverse direction,
//    should have better efficiency
package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"strconv"
	"time"
	got "github.com/yglcode/mesosgot"
)

//-------- Settings for elevator simulator -------
const (
	//default number of floors
	DefaultNumberOfFloors = 8
	//default number of elevators
	DefaultNumberOfElevators = 4
	//default duration to run simulation
	DefaultSimulationSeconds = 30
	//time elevator takes to move to next floor
	ElevatorMoveOneFloorTime = time.Second
	//time range for riders arrive
	FloorRiderArrivalInterval = 3 * time.Second
	//time interval to run scheduler
	SchedulerRunInterval = time.Second
)

//------------- Scheduler related definitions ------------------

//Elevator scheduler system:
//1. recv pickup reqs from floors
//2. schedule pickups to elevators
//3. recv status updates from elevators
//4. keep all channels communicating with elevators, floors
//5. report scheduler overall status
type Scheduler struct {
	numFloor int
	numElevator int
	elevStatus     []*schedMsg    //elevator status
	chanin  <-chan got.GoTaskMsg         //chan for recv pickup reqs from floors
	chanout chan<-got.GoTaskMsg //chans for recv elevator status updates
	waitUpQue      [][]*schedMsg           //queue of riders waiting going up
	waitDownQue    [][]*schedMsg           //queue of riders waiting going down
	schedTickChan  <-chan time.Time     //time ticks to invoke scheduling
}

func NewScheduler(nfloor, nelevator int, chin <-chan got.GoTaskMsg, chout chan<-got.GoTaskMsg) (s *Scheduler) {
	s = &Scheduler{
		nfloor,
		nelevator,
		make([]*schedMsg, nelevator),
		chin,
		chout,
		make([][]*schedMsg, nfloor),
		make([][]*schedMsg, nfloor),
		time.Tick(SchedulerRunInterval),
	}
	//init random number generator
	rand.Seed(time.Now().UnixNano())
	//
	for i:=0;i<nelevator;i++ {
		s.elevStatus[i]=&schedMsg{"elevator-"+strconv.Itoa(i),0,0}
	}
	return
}

//get elevator status report
func (s *Scheduler) Status() (res [][]int) {
	for i := 0; i < len(s.elevStatus); i++ {
		name := s.elevStatus[i].taskName
		pos := strings.LastIndex(name,"-")
		id := 0
		if pos>0 {
			id, _= strconv.Atoi(name[pos+1:])
		}
		res = append(res, []int{id, s.elevStatus[i].currFloor,
			s.elevStatus[i].goalFloor})
	}
	return
}

//get string representation of waiting que
func que2Str(wq [][]*schedMsg) string {
	var res []byte
	for _, v := range wq {
		res = append(res, []byte("[ ")...)
		for _, r := range v {
			res = append(res, []byte(fmt.Sprintf(" %v ", r))...)
		}
		res = append(res, []byte(" ]")...)
	}
	return string(res)
}

// Elevator simulator goroutine Run() func:
// 1. wait for pickup reqs from floors and status updates from elevators
// 2. from pickupReqChan, get all pickup reqs and schedule them to elevators
// 3. print out global status
// 4. orchestrate graceful shutdown of all goroutines
func (s *Scheduler) Run() {
	//setup exitchan
	exitChan := make(chan struct{})
	go func() {
		<-time.After(time.Duration(DefaultSimulationSeconds)*time.Second)
		close(exitChan)
	}()
	//start main logic
	log.Println("Elevator scheduler starts")
	reportCount := 1
	log.Printf("------------------ Period %d -----------------\n", reportCount)
SchedulerLoop:
	for {
		select {
		case <-exitChan:
			break SchedulerLoop //finish
		case <-s.schedTickChan:
			//print status before schedule
			log.Printf("elevators: %v\n", s.Status())
			log.Printf("waitUpQue: %v\n", que2Str(s.waitUpQue))
			log.Printf("waitDownQue: %v\n", que2Str(s.waitDownQue))
			//time to schedule
			s.schedule()
			//record next scheduling period
			reportCount++
			log.Printf("------------------ Period %d -----------------\n", reportCount)
		case m := <-s.chanin:
			msg := &schedMsg{}
			err := msg.decode(m)
			if err!=nil {
				log.Println("failed to decode msg: ",err)
				continue SchedulerLoop
			}
			taskType, idx, err := taskNameIndex(msg.taskName)
			switch {
			case taskType=="elevator":
				//update elevator status
				log.Printf("Scheduler recv elevator status update: %v\n", msg)
				s.elevStatus[idx] = msg
			case taskType=="floor":
				//recv pickup reqs from floors
				//log.Printf("Scheduler recv pickup req %v\n", r)
				if (msg.goalFloor - msg.currFloor) > 0 {
					s.waitUpQue[msg.currFloor] = append(s.waitUpQue[msg.currFloor], msg)
				} else {
					s.waitDownQue[msg.currFloor] = append(s.waitDownQue[msg.currFloor], msg)
				}
			}
		}
	}
	log.Println("Elevator scheduler exits")
}

//for better efficiency, schedule this way:
//1. keep elevators moving in one direction till end then reverse
//2. if there are riders waiting to go up, check if any up-going elevator take them
//3. if there are riders waiting to go down, check if any down-going elevator take them
//4. then check if any idle elevators can take them
//5. otherwise, riders wait in queue till idle elevators go to proper position to take them
func (s *Scheduler) schedule() {
	numWaitUp := 0
	numWaitDown := 0
	for _, v := range s.waitUpQue {
		numWaitUp += len(v)
	}
	for _, v := range s.waitDownQue {
		numWaitDown += len(v)
	}
	if numWaitUp == 0 && numWaitDown == 0 {
		log.Println("no schedule req")
		return
	}
	var upElevs []*schedMsg
	var downElevs []*schedMsg
	var idleElevs []*schedMsg
	for i := 0; i < len(s.elevStatus); i++ {
		switch {
		case s.elevStatus[i].goalFloor > s.elevStatus[i].currFloor:
			upElevs = append(upElevs, s.elevStatus[i])
		case s.elevStatus[i].goalFloor < s.elevStatus[i].currFloor:
			downElevs = append(downElevs, s.elevStatus[i])
		default:
			idleElevs = append(idleElevs, s.elevStatus[i])
		}
	}
	//log.Println("-0",  numWaitUp, numWaitDown, len(upElevs),len(downElevs),len(idleElevs))
	if numWaitUp > 0 {
		for _, el := range upElevs {
			for i := el.currFloor; i < s.numFloor && numWaitUp > 0; i++ {
				for j := 0; j < len(s.waitUpQue[i]); j++ {
					s.waitUpQue[i][j].taskName = el.taskName
					s.chanout <- s.waitUpQue[i][j].encode()
					numWaitUp--
				}
				s.waitUpQue[i] = nil
			}
		}
		//log.Println("-1", numWaitUp, numWaitDown, len(upElevs),len(downElevs),len(idleElevs))
		if numWaitUp > 0 {
			for _, el := range idleElevs {
				for i := el.currFloor; i < s.numFloor && numWaitUp > 0; i++ {
					for j := 0; j < len(s.waitUpQue[i]); j++ {
						s.waitUpQue[i][j].taskName = el.taskName
						s.chanout <- s.waitUpQue[i][j].encode()
						numWaitUp--
					}
					s.waitUpQue[i] = nil
				}
			}
		}
	}
	if numWaitDown > 0 {
		for _, el := range downElevs {
			for i := el.currFloor; i >= 0 && numWaitDown > 0; i-- {
				for j := 0; j < len(s.waitDownQue[i]); j++ {
					s.waitDownQue[i][j].taskName = el.taskName
					s.chanout <- s.waitDownQue[i][j].encode()
					numWaitDown--
				}
				s.waitDownQue[i] = nil
			}
		}
		if numWaitDown > 0 {
			for _, el := range idleElevs {
				for i := el.currFloor; i >= 0 && numWaitDown > 0; i-- {
					for j := 0; j < len(s.waitDownQue[i]); j++ {
						s.waitDownQue[i][j].taskName = el.taskName
						s.chanout <- s.waitDownQue[i][j].encode()
						numWaitDown--
					}
					s.waitDownQue[i] = nil
				}
			}
		}
	}
	if len(idleElevs) > 0 {
		pos := 0
		if numWaitUp > 0 {
			jumpBot := 0
			for i := 0; i < s.numFloor; i++ {
				if len(s.waitUpQue[i]) > 0 {
					jumpBot = i
					break
				}
			}
			msg := schedMsg{idleElevs[pos].taskName, jumpBot, jumpBot}
			s.chanout<-msg.encode()
			pos++
		}
		if len(idleElevs) > 1 && numWaitDown > 0 {
			jumpTop := 0
			for i := s.numFloor - 1; i >= 0; i-- {
				if len(s.waitDownQue[i]) > 0 {
					jumpTop = i
					break
				}
			}
			msg := schedMsg{idleElevs[pos].taskName, jumpTop, jumpTop}
			s.chanout<-msg.encode()
			pos++
		}

	}
}
