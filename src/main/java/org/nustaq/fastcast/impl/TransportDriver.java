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
package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.transport.PhysicalTransport;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/13/13
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class TransportDriver {

    public static int MAX_NUM_TOPICS = 256;

    int spinIdleLoopMicros = 1000*1000*10;
    int idleParkMicros = 500;

    volatile PhysicalTransport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];
    long lastMsg[]; // marks last flush on a topic to enable batch delay

    StructString nodeId;

    Thread receiverThread, houseKeeping;
    FSTStructAllocator alloc = new FSTStructAllocator(1);
    long autoFlushMS;
    private ConcurrentHashMap<Integer,Topic> topics = new ConcurrentHashMap<>();

    int tCheckCounter = 0;
    volatile int terminationCounter = 0;

    public TransportDriver(PhysicalTransport trans, String nodeId) {
        this.trans = trans;
        this.nodeId = alloc.newStruct( new StructString(nodeId) );
        final PhysicalTransportConf tconf = trans.getConf();
        this.autoFlushMS = tconf.getAutoFlushMS();
        this.spinIdleLoopMicros = tconf.getSpinLoopMicros();
        this.idleParkMicros = tconf.getIdleParkMicros();

        receiver = new ReceiveBufferDispatcher[MAX_NUM_TOPICS];
        sender = new PacketSendBuffer[MAX_NUM_TOPICS];
        lastMsg = new long[MAX_NUM_TOPICS];

        receiverThread = new Thread("trans receiver "+ tconf.getName()) {
            public void run() {
                receiveLoop();
            }
        };
        receiverThread.start();
        houseKeeping = new Thread("trans houseKeeping "+ tconf.getName()) {
            public void run() {
                houseKeepingLoop();
            }
        };
        houseKeeping.start();
    }

    private void installReceiver(Topic chan, FCSubscriber msgListener) {
        ReceiveBufferDispatcher receiveBufferDispatcher = new ReceiveBufferDispatcher(trans.getConf().getDgramsize(), nodeId.toString(), chan, msgListener);
        if ( receiver[chan.getTopicId()] != null ) {
            throw new RuntimeException("double usage of topic "+chan.getTopicId()+" on transport "+trans.getConf().getName() );
        }
        receiver[chan.getTopicId()] = receiveBufferDispatcher;
    }

    public boolean hasReceiver(int topicId) {
        return receiver[topicId] != null;
    }

    public boolean hasSender(int topicId) {
        return sender[topicId] != null;
    }

    /**
     * installs and initializes sender thread and buffer, sets is to topicEntry given in argument !!
     * @param topicEntry
     */
    private PacketSendBuffer installSender(final Topic topicEntry) {
        if ( sender[topicEntry.getTopicId()] != null ) {
            return sender[topicEntry.getTopicId()];
        }
        PacketSendBuffer packetSendBuffer = new PacketSendBuffer(trans, nodeId.toString(), topicEntry);
        sender[topicEntry.getTopicId()] = packetSendBuffer;
        topicEntry.setSender(packetSendBuffer);
        return packetSendBuffer;
    }

    long lastTimeoutCheck = System.currentTimeMillis();
    private void houseKeepingLoop() {
        ArrayList<String> lostSenders = new ArrayList<>();
        while (!isTerminated()) {
            try {
                long now = System.currentTimeMillis();
                if ( now - lastTimeoutCheck > 2000 ) { // timouts < 2 seconds not supported (and do not make sense ..)
                    lastTimeoutCheck = now;
                    for (int i = 0; i < receiver.length; i++) {
                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[i];
                        if (receiveBufferDispatcher != null) {
                            Topic topicEntry = receiveBufferDispatcher.getTopicEntry();
                            lostSenders.clear();
                            List<String> timedOutSenders = topicEntry.getTimedOutSenders(lostSenders, now, topicEntry.getHbTimeoutMS());
                            if (timedOutSenders != null && timedOutSenders.size() > 0) {
                                if ( ! isTerminated() )
                                    cleanup(timedOutSenders, i);
                            }
                        }
                    }
                }
                for (int i = 0; i < sender.length; i++) {
                    PacketSendBuffer packetSendBuffer = sender[i];
                    if ( packetSendBuffer != null ) {
                        long lastFlush = packetSendBuffer.lastMsgFlush;
                        if ( lastMsg[i] == 0 )
                            lastMsg[i] = lastFlush;
                        else if ( lastMsg[i] == lastFlush) {
                            // no flush since last turnaround, generate a flush
                            if ( ! isTerminated() )
                                packetSendBuffer.flush();
                        } else {
                            lastMsg[i] = lastFlush;
                        }
                    }
                }
                try {
                    Thread.sleep(autoFlushMS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        terminationCounter++;
    }

    private boolean isTerminated() {
        return trans == emptyTransport;
    }

    Packet receivedPacket;
    private void receiveLoop()
    {
        byte[] receiveBuf = new byte[trans.getConf().getDgramsize()];
        DatagramPacket p = new DatagramPacket(receiveBuf,receiveBuf.length);
        ByteBuffer buff = ByteBuffer.wrap(p.getData(), p.getOffset(), p.getLength());
        receivedPacket = alloc.newStruct(new Packet());
        long idleNanos = System.nanoTime();
        receivedPacket.baseOn(receiveBuf, 0);
        while(true) {
            boolean idle = true;
            try {
                buff.position(0);
                if (receiveDatagram(buff, receiveBuf)) {
                    idleNanos = System.nanoTime();
                    idle = false;
                } else {
                    if ( System.nanoTime()-idleNanos > spinIdleLoopMicros*1000) {
                        if ( ! trans.isBlocking() && idleParkMicros > 0)
                            LockSupport.parkNanos(1000*idleParkMicros);
                    } else {
                        idle = false;
                        if ( (ThreadLocalRandom.current().nextInt()&1) == 0 ) {
                            idleNanos++;
                        } else {
                            idleNanos--;
                        }
                    }
                }
                tCheckCounter++;
                if ( tCheckCounter == 100000 || idle || spinIdleLoopMicros == 0 ) {
                    tCheckCounter = 0;
                    if ( isTerminated() ) {
                        break;
                    }
                }
            } catch (Throwable e) {
                FCLog.log(e);
            }
        }
        while( terminationCounter < 1 ) { // wait for housekeeping to finish
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
        }
        alloc.free();
        for (int i = 0; i < receiver.length; i++) {
            ReceiveBufferDispatcher receiveBufferDispatcher = receiver[i];
            if ( receiveBufferDispatcher != null )
                receiveBufferDispatcher.cleanupTopic();
        }
    }

    public void terminate() {
        PhysicalTransport oldTrans = trans;
        trans = emptyTransport;
        oldTrans.close();
    }

    private boolean receiveDatagram(ByteBuffer p, byte wrappedArr[]) throws IOException {
        if ( trans.receive(p) ) {

            boolean selfSent = receivedPacket.getSender().equals(nodeId);
            if ( ! selfSent) {
                // debug
                Class debugtype = receivedPacket.getPointedClass();
                if ( debugtype == RetransPacket.class ) {
                    RetransPacket retransPacket = receivedPacket.cast().detach();
                    System.out.println("retrans received "+retransPacket);
                }
                //

                int topic = receivedPacket.getTopic();

                if ( topic > MAX_NUM_TOPICS || topic < 0 ) {
                    FCLog.get().warn("foreign traffic");
                    return true;
                }
                if ( receiver[topic] == null && sender[topic] == null) {
                    return true;
                }

                Class type = receivedPacket.getPointedClass();
                StructString receivedPacketReceiver = receivedPacket.getReceiver();
                if ( type == DataPacket.class )
                {
                    if ( receiver[topic] == null )
                        return true;
                    dispatchDataPacket(receivedPacket, topic);
                } else if ( type == RetransPacket.class )
                {
                    if ( sender[topic] == null )
                        return true;
                    if (receivedPacketReceiver == null || ! receivedPacketReceiver.equals(nodeId)) {
                        return true;
                    }
                    System.out.println("retrans dispatched ");
                    dispatchRetransmissionRequest(receivedPacket, topic);
                } else if (type == ControlPacket.class )
                {
                    if (isForeignReceiver(receivedPacketReceiver)) {
                        return true;
                    }
                    ControlPacket control = receivedPacket.cast();
                    if ( control.getType() == ControlPacket.DROPPED )
                    {
                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
                        if ( receiveBufferDispatcher != null ) {
                            FCLog.get().warn(nodeId+" has been dropped by "+receivedPacket.getSender()+" on service "+receiveBufferDispatcher.getTopicEntry().getTopicId());
                            FCSubscriber service = receiveBufferDispatcher.getTopicEntry().getSubscriber();
                            if ( service != null ) {
                                if ( service.dropped() ) { // retry if returns true
                                    FCLog.get().warn("..resyncing..");
                                    PacketReceiveBuffer buffer = receiveBufferDispatcher.getBuffer(receivedPacket.getSender());
                                    if ( buffer != null ) {
                                        buffer.resync();
                                    } else {
                                        FCLog.get().warn("unexpected null buffer");
                                    }
                                } else {
                                    // topic is lost forever now ..
                                    receiver[topic] = null;
                                    receiveBufferDispatcher.cleanupTopic();
                                }
                            }
                        }
                    }
                    // heartbeats are sent as regular data packets in order to have initialSync to happen correctly
//                    else if ( control.getType() == ControlPacket.HEARTBEAT ) {
//                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
//                        if ( receiveBufferDispatcher != null ) {
//                            PacketReceiveBuffer buffer = receiveBufferDispatcher.getBuffer(control.getSender());
//                            if ( buffer != null ) {
//                                buffer.updateHeartBeat(control.getSeqNo(),System.currentTimeMillis());
//                            }
//                        }
//                    }
                }
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    private boolean isForeignReceiver(StructString receivedPacketReceiver) {
        return receivedPacketReceiver != null && receivedPacketReceiver.getLen() > 0 && ! receivedPacketReceiver.equals(nodeId);
    }

    DataPacket tmpP;
    private void dispatchDataPacket(Packet receivedPacket, int topic) throws IOException {
        PacketReceiveBuffer buffer = receiver[topic].getBuffer(receivedPacket.getSender());
        tmpP = receivedPacket.cast().detachTo(tmpP); // avoid alloc
        RetransPacket retransPacket = buffer.receivePacket(tmpP);
        if ( retransPacket != null ) {
            // packet is valid just in this thread
            if ( PacketSendBuffer.RETRANSDEBUG )
                FCLog.get().info("send retrans request " + retransPacket + " " + retransPacket.getClzId());
            // FIXME: ALLOC
            trans.send(new DatagramPacket(retransPacket.getBase().toBytes(retransPacket.getOffset(), retransPacket.getByteSize()), 0, retransPacket.getByteSize()));
        }
    }

    private void dispatchRetransmissionRequest(Packet receivedPacket, int topic) throws IOException {
        RetransPacket retransPacket = (RetransPacket) receivedPacket.cast().detach();
        sender[topic].addRetransmissionRequest(retransPacket, trans);
    }

    void cleanup(List<String> timedOutSenders, int topic) {
        for (int i = 0; i < timedOutSenders.size(); i++) {
            String s = timedOutSenders.get(i);
            ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
//            receiver[topic] = null; wrong: disallows reconnect
            FCLog.get().info("stopped receiving heartbeats from "+s);
            if ( receiveBufferDispatcher != null ) {
                receiveBufferDispatcher.cleanup(s);
            }
        }
    }

    public void subscribe( String subsConf, FCSubscriber subscriber ) {
        subscribe(FastCast.getFastCast().getSubscriberConf(subsConf),subscriber);
    }

    public void subscribe( SubscriberConf subsConf, FCSubscriber subscriber ) {
        Topic topicEntry = topics.get(subsConf.getTopicId());
        if ( topicEntry == null )
            topicEntry = new Topic(null,null);
        if ( topicEntry.getPublisherConf() != null ) {
            throw new RuntimeException("already a sender registered at "+subsConf.getTopicId());
        }
        topicEntry.setSubscriberConf(subsConf);
        topicEntry.setChannelDispatcher(this);
        topicEntry.setSubscriber(subscriber);
        installReceiver(topicEntry, subscriber);
    }

    public FCPublisher publish( String pubConf ) {
        return publish(FastCast.getFastCast().getPublisherConf(pubConf));
    }

    public FCPublisher publish( PublisherConf pubConf ) {
        Topic topicEntry = topics.get(pubConf.getTopicId());
        if ( topicEntry == null )
            topicEntry = new Topic(null,null);
        if ( topicEntry.getPublisherConf() != null ) {
            throw new RuntimeException("already a sender registered at "+pubConf.getTopicId());
        }
        topicEntry.setChannelDispatcher(this);
        topicEntry.setPublisherConf(pubConf);
        topics.put(pubConf.getTopicId(), topicEntry);
        final PacketSendBuffer packetSendBuffer = installSender(topicEntry);
        return packetSendBuffer;
    }

    public ReceiveBufferDispatcher getReceiver(int topicId) {
        return receiver[topicId];
    }

    PhysicalTransport emptyTransport = new PhysicalTransport() {
        @Override
        public boolean receive(ByteBuffer pack) throws IOException {
            return false;
        }

        @Override
        public boolean receive(DatagramPacket pack) throws IOException {
            return false;
        }

        @Override
        public void send(DatagramPacket pack) throws IOException {

        }

        @Override
        public void send(byte[] bytes, int off, int len) throws IOException {

        }

        @Override
        public void send(ByteBuffer b) throws IOException {

        }

        @Override
        public void join() throws IOException {

        }

        @Override
        public PhysicalTransportConf getConf() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isBlocking() {
            return false;
        }
    };

}
