package main

import (
	"log"
	"time"
	got "github.com/yglcode/mesosgot"
)

//-------- Elevator definitions --------

//For best efficiency:
//1. elevator keep a local schedule of goal floors of all passengers
//2. elevator will run each direction till end & finish all then reverse
type Elevator struct {
	id         int
	taskName string
	currFloor  int
	goalFloor  int
	direction  int //+:up,-:down,0:idle
	localSched []bool
	schedChan  <-chan got.GoTaskMsg  //recv scheduling cmds from Scheduler
	statusChan chan<- got.GoTaskMsg //send status (currFloor,goalFloor) to Scheduler
}

func NewElevator(index int, tn string, mfloor int, schedCh <-chan got.GoTaskMsg, statusCh chan<- got.GoTaskMsg) (el *Elevator) {
	el = &Elevator{
		index,
		tn,
		0, 0, 0,
		make([]bool, mfloor),
		schedCh,
		statusCh,
	}
	return
}

//elevators' goroutine Run():
//1. consume scheduler commands from schedulers
//2. update its position by 1 floor
//3. do local schedule, pick up next goto floor
//4. recv exit cmd from Scheduler, cleanup & exit
func (el *Elevator) Run() {
	log.Printf("elevator[%d] starts\n", el.id)
	var r schedMsg
	var m got.GoTaskMsg
ElevatorLoop:
	for {
		//consume all scheduler & update local sched table
	schedcmd:
		for {
			select {
			case m = <-el.schedChan:
				if r.decode(m) != nil {
					log.Printf("failed to decode msg")
					continue schedcmd
				}
				if r.currFloor == -1 && r.goalFloor == -1 {
					break ElevatorLoop
				}
				log.Printf("elevator[%d] recv req [%d %d]\n", el.id, r.currFloor,r.goalFloor)
				el.localSched[r.currFloor] = true
				el.localSched[r.goalFloor] = true
			default:
				if el.WillMove() {
					break schedcmd
				} else {
					//no schedule, will not move, block wait schedule
					m = <-el.schedChan
					if r.decode(m) != nil {
						log.Printf("failed to decode msg")
						continue schedcmd
					}
					if r.currFloor == -1 && r.goalFloor == -1 {
						break ElevatorLoop
					}
					log.Printf("elevator[%d] recv req [%d %d]\n", el.id, r.currFloor, r.goalFloor)
					el.localSched[r.currFloor] = true
					el.localSched[r.goalFloor] = true
					continue schedcmd
				}
			}
		}
		//do local schdule, possible update goalFloor
		el.schedule()
		//update its location
		switch {
		case el.direction > 0:
			el.GoUpOneFloor()
		case el.direction < 0:
			el.GoDownOneFloor()
		}
		//if reach goal, find new goal floor
		if el.currFloor == el.goalFloor {
			//reach goal
			el.localSched[el.goalFloor] = false
			el.schedule()
		}
		//report status change to scheduler
		sm := schedMsg{el.taskName, el.currFloor, el.goalFloor}
		el.statusChan <- sm.encode()
	}
	log.Printf("elevator[%d] exits\n", el.id)
}

//is elevator currently moving or scheduled to move
func (el *Elevator) WillMove() bool {
	if el.currFloor != el.goalFloor { //is moving
		return true
	}
	for _, v := range el.localSched {
		if v { //has scheduled
			return true
		}
	}
	return false
}

func (el *Elevator) GoUpOneFloor() {
	//simulate moving distance by 1 sec
	time.Sleep(ElevatorMoveOneFloorTime)
	el.currFloor++
}

func (el *Elevator) GoDownOneFloor() {
	//simulate moving distance by 1 sec
	time.Sleep(ElevatorMoveOneFloorTime)
	el.currFloor--
}

//elevator local scheduling based on local table
func (el *Elevator) schedule() {
	upNext := -1
	for i := el.currFloor + 1; i < len(el.localSched); i++ {
		if el.localSched[i] {
			upNext = i
			break
		}
	}
	downNext := -1
	for i := el.currFloor - 1; i >= 0; i-- {
		if el.localSched[i] {
			downNext = i
			break
		}
	}
	switch {
	case el.direction > 0:
		//currently going up
		switch {
		case upNext >= 0:
			el.goalFloor = upNext
		case downNext >= 0:
			el.goalFloor = downNext
			el.direction = -1 //change direction
		default:
			el.direction = 0
		}
	case el.direction < 0:
		//currently going down
		switch {
		case downNext >= 0:
			el.goalFloor = downNext
		case upNext >= 0:
			el.goalFloor = upNext
			el.direction = 1 //change direction
		default:
			el.direction = 0
		}
	default:
		//currently idle
		switch {
		case upNext >= 0:
			el.goalFloor = upNext
			el.direction = 1
		case downNext >= 0:
			el.goalFloor = downNext
			el.direction = -1
		default:
			el.direction = 0
		}
	}
}
