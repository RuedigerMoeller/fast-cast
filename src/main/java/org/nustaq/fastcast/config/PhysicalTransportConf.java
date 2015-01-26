/**
 * Copyright (c) 2014, Ruediger Moeller. All rights reserved.
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
 * Date: 03.01.14
 * Time: 21:19
 * To change this template use File | Settings | File Templates.
 */
package org.nustaq.fastcast.config;

import org.nustaq.fastcast.util.FCLog;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

/**
 */
public class PhysicalTransportConf {

    String name;

    // determines the max size of a datagram sent.
    // use value near [MTU-100] for lowest latency (requires high end hardware), use 4k .. 16k for throughput (don't flush each msg then)
    int dgramsize = 3*1400-200;
    // interface used
    String interfaceAddr = "lo";
    // mcast address
    String multicastAddr = "229.9.9.9";
    // port
    int port = 45555;
    // see scoket options
    int trafficClass = 0x08;
    // set to true in order to receive packets on other processes on the same system
    boolean loopBack = true;
    // see socket options
    int ttl = 8;
    // receive and sendbuffer sizes. Don't make them too large as this queues up retransmission requests !!
    int socketReceiveBufferSize = 128_000;
    int socketSendBufferSize = 128_000;

    // time until a msg sent with flush=false is automatically flushed out
    // (batching for throughput)
    long autoFlushMS = 1;
    // number of idle receveive loops until spinlock is stopped to free CPU ressources
    // zero => us blocking mode to avoid burning cpu
    int spinLoopMicros = 0;
    // how long receiving thread will be haltet once idle. (high values => higher latency after idle, lower values => more cpu burn)
    // this option is valid only if spinLoopMicors > 0
    int idleParkMicros = 300;

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

    public String getInterfaceAddr() {
        if ( ! Character.isDigit(interfaceAddr.charAt(0)) ) {
            Enumeration<NetworkInterface> nets = null;
            try {
                nets = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface netint : Collections.list(nets)) {
                    if ( netint.getDisplayName().equalsIgnoreCase(interfaceAddr) ) {
                        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                        if ( inetAddresses.hasMoreElements() ) {
                            interfaceAddr = inetAddresses.nextElement().getHostAddress();
                            break;
                        } else {
                            FCLog.get().warn("specified interface " + interfaceAddr + " does not have an IP assigned");
                        }
                    }
                }
            } catch (SocketException e) {
                FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return interfaceAddr;
    }

    public PhysicalTransportConf interfaceAdr(String ifacAdr) {
        this.interfaceAddr = ifacAdr;
        return this;
    }

    public String getMulticastAddr() {
        return multicastAddr;
    }

    public PhysicalTransportConf mulitcastAdr(String mcastAdr) {
        this.multicastAddr = mcastAdr;
        return this;
    }

    public int getPort() {
        return port;
    }

    public PhysicalTransportConf port(int port) {
        this.port = port;
        return this;
    }

    public int getTrafficClass() {
        return trafficClass;
    }

    public PhysicalTransportConf trafficClass(int trafficClass) {
        this.trafficClass = trafficClass;
        return this;
    }

    public boolean isLoopBack() {
        return loopBack;
    }

    public PhysicalTransportConf loopBack(boolean loopBack) {
        this.loopBack = loopBack;
        return this;
    }

    public int getTtl() {
        return ttl;
    }

    public PhysicalTransportConf ttl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public PhysicalTransportConf socketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
        return this;
    }

    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public PhysicalTransportConf socketSendBufferSize(int sendBufferSize) {
        this.socketSendBufferSize = sendBufferSize;
        return this;
    }

    public long getAutoFlushMS() {
        return autoFlushMS;
    }

    public int getIdleParkMicros() {
        return idleParkMicros;
    }

    public int getSpinLoopMicros() {
        return spinLoopMicros;
    }

    public PhysicalTransportConf autoFlushMS(long autoFlushMS) {
        this.autoFlushMS = autoFlushMS;
        return this;
    }

    public PhysicalTransportConf idleParkMicros(int idleParkMicros) {
        this.idleParkMicros = idleParkMicros;
        return this;
    }

    public PhysicalTransportConf spinLoopMicros(int spinLoopMicros) {
        this.spinLoopMicros = spinLoopMicros;
        return this;
    }
}
