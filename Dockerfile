# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Yuyi Guo, <yuyi@fnal.gov>, 2021


FROM golang:latest as go-builder

# build procedure
ENV WDIR=/data
WORKDIR ${WDIR}
# RUN mkdir -p /data/{stompserver,gopath, etc, run} && mkdir /build
RUN mkdir -p /data/gopath
RUN mkdir /build
ENV GOPATH=/data/gopath
ARG CGO_ENABLED=0
# get go libraries
# RUN go get github.com/lestrrat-go/file-rotatelogs
RUN go get github.com/vkuznet/lb-stomp
RUN go get github.com/go-stomp/stomp
#RUN go get github.com/dmwm/RucioTracers
# build Rucio tracer
WORKDIR ${WDIR} 
RUN git clone https://github.com/dmwm/RucioTracers.git
WORKDIR ${WDIR}/RucioTracers/stompserver
RUN make
#RUN curl -ksLO https://raw.githubusercontent.com/dmwm/RucioTracers/main/stompserver/stompserver.go
#RUN go mod init github.com/dmwm/RucioTracers/stompserver && go mod tidy && \
    #go build -o /build/RucioTracer -ldflags="-s -w -extldflags -static" /data/stompserver/stompserver.go
FROM alpine
# when COPY, need full path, ${WDIR}/RucioTracers/stompserver/RucioTracer will nor wor, WHY?
COPY --from=go-builder /data/RucioTracers/stompserver/RucioTracer /data/
RUN mkdir -p /data/run
#
COPY --from=go-builder /data/RucioTracers/run.sh /data/run/
RUN mkdir -p /data/etc
COPY --from=go-builder /data/RucioTracers/etc/ruciositemap.json /data/etc/
