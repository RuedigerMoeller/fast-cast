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

    int dgramsize = 1300;
    String ifacAdr = "lo";
    String mcastAdr = "229.9.9.9";
    int port = 45555;
    int trafficClass = 0x08;
    boolean loopBack = true;
    int ttl = 2;
    int socketReceiveBufferSize = 30000000; // used as file size for shmem
    int sendBufferSize = 8000000;
    String queueFile; // for shared mem, identifies file path of mmapped file for this transport
    private long autoFlushMS = 3;

    public PhysicalTransportConf() {
    }

    public PhysicalTransportConf(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDgramsize() {
        return dgramsize;
    }

    public void setDgramsize(int dgramsize) {
        this.dgramsize = dgramsize;
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

    public void setMcastAdr(String mcastAdr) {
        this.mcastAdr = mcastAdr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(int trafficClass) {
        this.trafficClass = trafficClass;
    }

    public boolean isLoopBack() {
        return loopBack;
    }

    public void setLoopBack(boolean loopBack) {
        this.loopBack = loopBack;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public String getQueueFile() {
        return queueFile;
    }

    public void setQueueFile(String queueFile) {
        this.queueFile = queueFile;
    }

    public long getAutoFlushMS() {
        return autoFlushMS;
    }
}
