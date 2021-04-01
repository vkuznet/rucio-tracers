#!/bin/sh
cd /data
echo $PWD
ls
ls /etc/secrets
ls -l /data/RucioTracer
/data/RucioTracer -help
/data/RucioTracer -config /etc/secrets/stompserverconfig.json -sitemap /data/etc/ruciositemap.json &
sleep 1200
killall RucioTracer
