/*
 * Copyright 2014 Ruediger Moeller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    // receive and sendbuffer sizes. Try to get large ones ..
    int socketReceiveBufferSize = 8_000_000;
    int socketSendBufferSize = 8_000_000;

    // time until a msg sent with flush=false is automatically flushed out
    // (batching for throughput)
    long autoFlushMS = 3;
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
