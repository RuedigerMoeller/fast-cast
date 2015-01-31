#!/bin/bash

# use nicstat.sh to monitor pps

if [ -z "$1" ]; then
        echo
        echo usage: $0 [network-interface] [sendrate e.g. 1000]
        echo
        echo e.g. $0 eth0
        echo
        echo starts iperf udp bench server
        exit
fi

route add -host 239.255.1.3 $1
iperf -c 239.255.1.3 -u -b $2m -f m -i 5 -t 60
