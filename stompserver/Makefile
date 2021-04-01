VERSION=`git rev-parse --short HEAD`
flags=-ldflags="-s -w -X main.version=${VERSION}"

all: build

build:
	go clean; rm -rf pkg; go build -o RucioTracer ${flags}

build_debug:
	go clean; rm -rf pkg; go build -o RucioTracer ${flags} -gcflags="-m -m"

build_all: build_osx build_linux build

build_osx:
	go clean; rm -rf pkg RucioTracer_osx; GOOS=darwin go build -o RucioTracer ${flags}

build_linux:
	go clean; rm -rf pkg RucioTracer_linux; GOOS=linux go build -o RucioTracer ${flags}

build_power8:
	go clean; rm -rf pkg RucioTracer_power8; GOARCH=ppc64le GOOS=linux go build -o RucioTracer ${flags}

build_arm64:
	go clean; rm -rf pkg RucioTracer_arm64; GOARCH=arm64 GOOS=linux go build -o RucioTracer ${flags}

build_windows:
	go clean; rm -rf pkg RucioTracer.exe; GOARCH=amd64 GOOS=windows go build -o RucioTracer ${flags}

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	go test