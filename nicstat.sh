#!/bin/bash

INTERVAL="1"  # update interval in seconds

if [ -z "$1" ]; then
        echo
        echo usage: $0 [network-interface]
        echo
        echo e.g. $0 eth0
        echo
        echo shows packets-per-second
        exit
fi

IF=$1

while true
do
        R1=`cat /sys/class/net/$1/statistics/rx_packets`
        T1=`cat /sys/class/net/$1/statistics/tx_packets`
        RB1=`cat /sys/class/net/$1/statistics/rx_bytes`
        TB1=`cat /sys/class/net/$1/statistics/tx_bytes`
        sleep $INTERVAL
        R2=`cat /sys/class/net/$1/statistics/rx_packets`
        T2=`cat /sys/class/net/$1/statistics/tx_packets`
        RB2=`cat /sys/class/net/$1/statistics/rx_bytes`
        TB2=`cat /sys/class/net/$1/statistics/tx_bytes`
        TXPPS=`expr $T2 - $T1`
        RXPPS=`expr $R2 - $R1`
        TXBPS=`expr $TB2 - $TB1`
        RXBPS=`expr $RB2 - $RB1`
	TXBPS=`expr $TXBPS / 1000`
	RXBPS=`expr $RXBPS / 1000`
        echo "TX $1: $TXPPS pkts/s RX $1: $RXPPS pkts/s TB $1: $TXBPS kb/s RB $1: $RXBPS kb/s"
done
