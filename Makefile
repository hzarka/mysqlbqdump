
test:
	go build && ./mysqlbqdump --epoch=false --format=json test test 

build:
	CGO_ENABLED=0 go build -a -installsuffix cgo

