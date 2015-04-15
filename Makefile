EXAMPLE = example
ELEVATOR = elevator

all: elevator example

example: example-scheduler example-executor

example-scheduler:
	rm -rf ${EXAMPLE}/$@
	go build -o ${EXAMPLE}/$@ ${EXAMPLE}/example_scheduler.go ${EXAMPLE}/common.go

example-executor:
	rm -rf ${EXAMPLE}/$@
	go build -o ${EXAMPLE}/$@ ${EXAMPLE}/example_executor.go ${EXAMPLE}/common.go

elevator: elevator-scheduler elevator-executor

elevator-scheduler:
	rm -rf ${ELEVATOR}/$@
	go build -o ${ELEVATOR}/$@ ${ELEVATOR}/elevator_scheduler.go ${ELEVATOR}/scheduler.go  ${ELEVATOR}/common.go

elevator-executor:
	rm -rf ${ELEVATOR}/$@
	go build -o ${ELEVATOR}/$@ ${ELEVATOR}/elevator_executor.go ${ELEVATOR}/scheduler.go ${ELEVATOR}/elevator.go ${ELEVATOR}/floor.go ${ELEVATOR}/common.go

