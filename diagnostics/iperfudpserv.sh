#!/bin/bash

if [ -z "$1" ]; then
        echo
        echo usage: $0 [network-interface]
        echo
        echo e.g. $0 eth0
        echo
        echo starts iperf udp bench server
        exit
fi

route add -host 239.255.1.3 $1
iperf -s -B 239.255.1.3 -u -f m -i 5
