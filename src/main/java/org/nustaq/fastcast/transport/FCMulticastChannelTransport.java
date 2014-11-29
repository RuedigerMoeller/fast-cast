package org.nustaq.fastcast.transport;

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
 * To change this template use File | Settings | File Templates.
 */
public class FCMulticastChannelTransport implements Transport {

    DatagramChannel receiveSocket;
    DatagramChannel sendSocket;
    FCSocketConf conf;
    NetworkInterface iface;
    InetAddress address;
    InetSocketAddress socketAddress;

    public FCMulticastChannelTransport(FCSocketConf conf) {
        System.setProperty("java.net.preferIPv4Stack","true" );
        this.conf = conf;
    }

    public boolean receive(DatagramPacket pack) throws IOException {
        SocketAddress receive = receiveSocket.receive(ByteBuffer.wrap(pack.getData(), pack.getOffset(), pack.getLength()));
        if ( receive instanceof InetSocketAddress) {
            pack.setAddress(((InetSocketAddress) receive).getAddress());
        }
        return receive != null;
    }

    public void send(DatagramPacket pack) throws IOException {
        sendSocket.send(ByteBuffer.wrap(pack.getData(), pack.getOffset(), pack.getLength()), socketAddress);
    }

    public InetSocketAddress getAddress() {
        return socketAddress;
    }

    public NetworkInterface getInterface() {
        return iface;
    }

    public void join() throws IOException {
        if ( address == null ) {
            address = InetAddress.getByName(conf.mcastAdr);
        }
        socketAddress = new InetSocketAddress(address,conf.port);
        if ( iface == null && conf.getIfacAdr() != null) {
            iface =NetworkInterface.getByInetAddress(Inet4Address.getByName(conf.getIfacAdr() ));
            if ( iface == null ) {
                iface = NetworkInterface.getByInetAddress( Inet4Address.getByName(conf.getIfacAdr() ));
            }
            if ( iface == null ) {
                FCLog.log("Could not find a network interface named '" + conf.getIfacAdr() + "'");
            }
        }
        receiveSocket = ceateSocket();
        sendSocket = ceateSocket();

        MembershipKey key = receiveSocket.join(address, iface);
        FCLog.log("Connecting to interface " + iface.getName()+ " on address " + address + " " + conf.port+" dgramsize:"+getConf().getDgramsize());

    }

    private DatagramChannel ceateSocket() throws IOException {
        DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setOption(StandardSocketOptions.IP_MULTICAST_IF, iface)
                .setOption(StandardSocketOptions.SO_RCVBUF, conf.receiveBufferSize)
                .setOption(StandardSocketOptions.IP_TOS, conf.trafficClass)
                .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, conf.loopBack)
                .setOption(StandardSocketOptions.IP_MULTICAST_TTL, conf.ttl)
                .bind(new InetSocketAddress(conf.port));
        channel.configureBlocking(true);
        return channel;
    }

    @Override
    public FCSocketConf getConf() {
        return conf;
    }
}
