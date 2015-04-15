package main

import (
	"log"
	"math/rand"
	"time"
	got "github.com/yglcode/mesosgot"
)

//---------- Floor related definitions --------------

//Floor:
// generate random pickup requests for scheduler
// each req is a rider obj
type Floor struct {
	id            int         //floor number
	taskName      string
	numFloor      int         //number floors at building
	exitChan      <-chan got.GoTaskMsg   //Scheduler send "exit" cmd on this chan
	pickupReqChan chan<- got.GoTaskMsg //send pickup req to scheduler
}

func NewFloor(index int, tn string, numF int, exitCh <-chan got.GoTaskMsg, pickChan chan<-got.GoTaskMsg) *Floor {
	return &Floor{
		index,
		tn,
		numF,
		exitCh,
		pickChan}
}

//Floor goroutine Run() func;
//  1. sleep random interval (0-3 secs)
//  2. generate random pickup req
func (f *Floor) Run() {
	log.Printf("floor[%d] starts\n", f.id)
FloorLoop:
	for {
		select {
		case <-f.exitChan:
			break FloorLoop
		case <-time.After(time.Duration(rand.Float64() * (float64)(FloorRiderArrivalInterval))):
		}
		//let floor generate random requests
		goalFloor := rand.Intn(f.numFloor)
		if f.id != goalFloor {
			m := schedMsg{f.taskName, f.id, goalFloor}
			f.pickupReqChan <- m.encode()
			log.Printf("pickup req: floor[%d], dst[%d]\n", f.id, goalFloor)
		} else {
			log.Printf("No pickup req: floor[%d]\n", f.id)
		}
	}
	//tell scheduler i exits
	log.Printf("floor[%d] exits\n", f.id)
}
