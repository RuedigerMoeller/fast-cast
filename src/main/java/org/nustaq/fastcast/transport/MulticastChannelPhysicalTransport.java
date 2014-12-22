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
package org.nustaq.fastcast.transport;

import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.util.FCLog;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 06.06.13
 * Time: 13:45
 *
 * ImMplements
 */
public class MulticastChannelPhysicalTransport implements PhysicalTransport {

    boolean blocking;
    DatagramChannel receiveSocket;
    DatagramChannel sendSocket;
    PhysicalTransportConf conf;
    NetworkInterface iface;
    InetAddress address;
    InetSocketAddress socketAddress;

    public MulticastChannelPhysicalTransport(PhysicalTransportConf conf, boolean blocking) {
        System.setProperty("java.net.preferIPv4Stack","true" );
        this.conf = conf;
        this.blocking = blocking;
    }

    public boolean receive(DatagramPacket pack) throws IOException {
        SocketAddress receive = receiveSocket.receive(ByteBuffer.wrap(pack.getData(), pack.getOffset(), pack.getLength()));
        if ( receive instanceof InetSocketAddress) {
            pack.setAddress(((InetSocketAddress) receive).getAddress());
        }
        return receive != null;
    }

    public boolean receive(ByteBuffer pack) throws IOException {
        SocketAddress receive = receiveSocket.receive(pack);
        return receive != null;
    }

    @Override
    public void send(DatagramPacket pack) throws IOException {
        sendSocket.send(ByteBuffer.wrap(pack.getData(), pack.getOffset(), pack.getLength()), socketAddress);
    }

    @Override
    public void send(byte[] bytes, int off, int len) throws IOException {
        sendSocket.send(ByteBuffer.wrap(bytes, off, len), socketAddress);
    }

    @Override
    public void send(ByteBuffer b) throws IOException {
//        long len = b.remaining();
        sendSocket.send(b, socketAddress);
    }

    public InetSocketAddress getAddress() {
        return socketAddress;
    }

    public NetworkInterface getInterface() {
        return iface;
    }

    public void join() throws IOException {
        if ( address == null ) {
            address = InetAddress.getByName(conf.getMulticastAddr());
        }
        socketAddress = new InetSocketAddress(address,conf.getPort());
        if ( iface == null && conf.getInterfaceAddr() != null) {
            iface =NetworkInterface.getByInetAddress(Inet4Address.getByName(conf.getInterfaceAddr() ));
            if ( iface == null ) {
                iface = NetworkInterface.getByInetAddress( Inet4Address.getByName(conf.getInterfaceAddr() ));
            }
            if ( iface == null ) {
                FCLog.log("Could not find a network interface named '" + conf.getInterfaceAddr() + "'");
            }
        }
        receiveSocket = ceateSocket(blocking);
        sendSocket = ceateSocket(false);

        MembershipKey key = receiveSocket.join(address, iface);
        FCLog.log("Connecting to interface " + iface.getName()+ " on address " + address + " " + conf.getPort()+" dgramsize:"+getConf().getDgramsize());

    }

    private DatagramChannel ceateSocket(boolean block) throws IOException {
        DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setOption(StandardSocketOptions.IP_MULTICAST_IF, iface)
                .setOption(StandardSocketOptions.SO_RCVBUF, conf.getSocketReceiveBufferSize())
                .setOption(StandardSocketOptions.IP_TOS, conf.getTrafficClass())
                .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, conf.isLoopBack())
                .setOption(StandardSocketOptions.IP_MULTICAST_TTL, conf.getTtl())
                .bind(new InetSocketAddress(conf.getPort()));
        channel.configureBlocking(block);
        return channel;
    }

    @Override
    public PhysicalTransportConf getConf() {
        return conf;
    }

    @Override
    public void close() {
        try {
            if ( receiveSocket != null )
                receiveSocket.close();
            receiveSocket = null;
        } catch (IOException e) {
            FCLog.get().warn(e);
        }
        try {
            if ( sendSocket != null )
                sendSocket.close();
            sendSocket = null;
        } catch (IOException e) {
            FCLog.get().warn(e);
        }
    }

    @Override
    public boolean isBlocking() {
        return blocking;
    }
}
