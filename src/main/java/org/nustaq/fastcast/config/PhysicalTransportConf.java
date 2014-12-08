package org.nustaq.fastcast.config;

import org.nustaq.fastcast.util.FCLog;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 *
 * Date: 05.06.13
 * Time: 18:27
 * To change this template use File | Settings | File Templates.
 */
public class PhysicalTransportConf {

    String name;

    // determines the max size of a datagram sent.
    // use value near [MTU-100] for lowest latency (requires high end hardware), use 4k .. 16k for throughput (don't flush each msg then)
    int dgramsize = 1300;
    // interface used
    String ifacAdr = "lo";
    // mcast address
    String mcastAdr = "229.9.9.9";
    // port
    int port = 45555;
    // see scoket options
    int trafficClass = 0x08;
    // set to true in order to receive packets on other processes on the same system
    boolean loopBack = true;
    // see socket options
    int ttl = 2;
    // receive and sendbuffer sizes. Try to get large ones ..
    int socketReceiveBufferSize = 30_000_000;
    int sendBufferSize = 8_000_000;

    // time until a msg sent with flush=false is automatically flushed out
    // (batching for throughput)
    long autoFlushMS = 3;
    // number of idle receveive loops until spinlock is stopped to free CPU ressources
    int spinIdleLoopMax = 10_000_000;
    // how long receiving thread will be haltet once idle. (high values => higher latency after idle, lower values => more cpu burn)
    int idleParkMicros = 500;

    public PhysicalTransportConf() {
    }

    public PhysicalTransportConf(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public PhysicalTransportConf setName(String name) {
        this.name = name;
        return this;
    }

    public int getDgramsize() {
        return dgramsize-4; // account for quirksy computation errors
    }

    public PhysicalTransportConf setDgramsize(int dgramsize) {
        this.dgramsize = dgramsize;
        return this;
    }

    public String getIfacAdr() {
        if ( ! Character.isDigit(ifacAdr.charAt(0)) ) {
            Enumeration<NetworkInterface> nets = null;
            try {
                nets = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface netint : Collections.list(nets)) {
                    if ( netint.getDisplayName().equalsIgnoreCase(ifacAdr) ) {
                        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                        if ( inetAddresses.hasMoreElements() ) {
                            ifacAdr = inetAddresses.nextElement().getHostAddress();
                            break;
                        } else {
                            FCLog.get().warn("specified interface " + ifacAdr + " does not have an IP assigned");
                        }
                    }
                }
            } catch (SocketException e) {
                FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return ifacAdr;
    }

    public PhysicalTransportConf setIfacAdr(String ifacAdr) {
        this.ifacAdr = ifacAdr;
        return this;
    }

    public String getMcastAdr() {
        return mcastAdr;
    }

    public PhysicalTransportConf setMcastAdr(String mcastAdr) {
        this.mcastAdr = mcastAdr;
        return this;
    }

    public int getPort() {
        return port;
    }

    public PhysicalTransportConf setPort(int port) {
        this.port = port;
        return this;
    }

    public int getTrafficClass() {
        return trafficClass;
    }

    public PhysicalTransportConf setTrafficClass(int trafficClass) {
        this.trafficClass = trafficClass;
        return this;
    }

    public boolean isLoopBack() {
        return loopBack;
    }

    public PhysicalTransportConf setLoopBack(boolean loopBack) {
        this.loopBack = loopBack;
        return this;
    }

    public int getTtl() {
        return ttl;
    }

    public PhysicalTransportConf setTtl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public PhysicalTransportConf setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
        return this;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public PhysicalTransportConf setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public long getAutoFlushMS() {
        return autoFlushMS;
    }

    public int getIdleParkMicros() {
        return idleParkMicros;
    }

    public int getSpinIdleLoopMax() {
        return spinIdleLoopMax;
    }

    public PhysicalTransportConf setAutoFlushMS(long autoFlushMS) {
        this.autoFlushMS = autoFlushMS;
        return this;
    }

    public PhysicalTransportConf setIdleParkMicros(int idleParkMicros) {
        this.idleParkMicros = idleParkMicros;
        return this;
    }

    public PhysicalTransportConf setSpinIdleLoopMax(int spinIdleLoopMax) {
        this.spinIdleLoopMax = spinIdleLoopMax;
        return this;
    }
}
