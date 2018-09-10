
test: testavro testjson

testbuild:
	go build

testavro: testbuild
	./mysqlbqdump --epoch=false --format=avro test test_a  > tmp/test_a.avro
	./mysqlbqdump --epoch=false --format=avro test test_b  > tmp/test_b.avro
	./mysqlbqdump --epoch=false --format=avro test test_c  > tmp/test_c.avro
	fastavro tmp/test_a.avro
	fastavro tmp/test_b.avro

testjson: testbuild
	./mysqlbqdump --epoch=false --format=json test test_a > tmp/test_a.json
	./mysqlbqdump --epoch=false --format=json test test_b > tmp/test_b.json
	./mysqlbqdump --epoch=false --format=json test test_c > tmp/test_c.json
	cat tmp/test_a.json
	cat tmp/test_b.json
	cat tmp/test_c.json

build:
	CGO_ENABLED=0 go build -a -installsuffix cgo

