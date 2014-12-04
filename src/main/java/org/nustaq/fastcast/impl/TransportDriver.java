package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.api.FCPublisher;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/13/13
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class TransportDriver {

    public static final int IDLE_SPIN_IDLE_COUNT = 1000*1000*10;
    private static final int IDLE_SLEEP_MICRO_SECONDS = 1000;
    public static int MAX_NUM_TOPICS = 256;
    PhysicalTransport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];
    long lastMsg[]; // marks last flush on a topic to enable batch delay

    StructString clusterName;
    StructString nodeId;

    Thread receiverThread, houseKeeping;
    FSTStructAllocator alloc = new FSTStructAllocator(1);
    long autoFlushMS;
    private ConcurrentHashMap<Integer,Topic> topics = new ConcurrentHashMap<>();

    public TransportDriver(PhysicalTransport trans, String clusterName, String nodeId) {
        this.trans = trans;
        this.nodeId = alloc.newStruct( new StructString(nodeId) );
        this.clusterName = alloc.newStruct( new StructString(clusterName) );
        this.autoFlushMS = trans.getConf().getAutoFlushMS();

        receiver = new ReceiveBufferDispatcher[MAX_NUM_TOPICS];
        sender = new PacketSendBuffer[MAX_NUM_TOPICS];
        lastMsg = new long[MAX_NUM_TOPICS];

        receiverThread = new Thread("trans receiver "+trans.getConf().getName()) {
            public void run() {
                receiveLoop();
            }
        };
        receiverThread.start();
        houseKeeping = new Thread("trans houseKeeping"+trans.getConf().getName()) {
            public void run() {
                houseKeepingLoop();
            }
        };
        houseKeeping.start();
    }

    private void installReceiver(Topic chan, FCSubscriber msgListener) {
        ReceiveBufferDispatcher receiveBufferDispatcher = new ReceiveBufferDispatcher(trans.getConf().getDgramsize(), clusterName.toString(), nodeId.toString(), chan, msgListener);
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
        PacketSendBuffer packetSendBuffer = new PacketSendBuffer(trans, clusterName.toString(), nodeId.toString(), topicEntry);
        sender[topicEntry.getTopicId()] = packetSendBuffer;
        topicEntry.setSender(packetSendBuffer);
        return packetSendBuffer;
    }

    private void houseKeepingLoop() {
        while ( true ) {
            try {
                long now = System.currentTimeMillis();
                for (int i = 0; i < receiver.length; i++) {
                    ReceiveBufferDispatcher receiveBufferDispatcher = receiver[i];
                    if ( receiveBufferDispatcher != null ) {
                        Topic topicEntry = receiveBufferDispatcher.getTopicEntry();
                        List<String> timedOutSenders = topicEntry.getTimedOutSenders(now, topicEntry.getHbTimeoutMS());
                        if ( timedOutSenders != null && timedOutSenders.size() > 0 ) {
                            cleanup(timedOutSenders,i);
                            topicEntry.removeSenders(timedOutSenders);
                        }
                    }
                }
                for (int i = 0; i < sender.length; i++) {
                    PacketSendBuffer packetSendBuffer = sender[i];
                    if ( packetSendBuffer != null ) {
                        long lastFlush = packetSendBuffer.lastFlush;
                        if ( lastMsg[i] == 0 )
                            lastMsg[i] = lastFlush;
                        else if ( lastMsg[i] == lastFlush) {
                            // no flush since last turnaround, generate a flush
                            packetSendBuffer.offer(null,0,0,true);
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
    }

    Packet receivedPacket;
    private void receiveLoop()
    {
        byte[] receiveBuf = new byte[trans.getConf().getDgramsize()];
        DatagramPacket p = new DatagramPacket(receiveBuf,receiveBuf.length);
        ByteBuffer buff = ByteBuffer.wrap(p.getData(), p.getOffset(), p.getLength());
        receivedPacket = alloc.newStruct(new Packet());
        int idleCount = 0;
        receivedPacket.baseOn(receiveBuf, 0);
        while(true) {
            try {
                buff.position(0);
                if (receiveDatagram(buff, receiveBuf)) {
                    idleCount = 0;
                } else {
                    idleCount++;
                    if ( idleCount > IDLE_SPIN_IDLE_COUNT ) {
                        LockSupport.parkNanos(1000*IDLE_SLEEP_MICRO_SECONDS);
                    }
                }
            } catch (IOException e) {
                FCLog.log(e);
            }
        }
    }

    private boolean receiveDatagram(ByteBuffer p, byte wrappedArr[]) throws IOException {
        if ( trans.receive(p) ) {

            boolean sameCluster = receivedPacket.getCluster().equals(clusterName);
            boolean selfSent = receivedPacket.getSender().equals(nodeId);

            if ( sameCluster && ! selfSent) {

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
                    if (
                        ( receivedPacketReceiver == null || receivedPacketReceiver.getLen() == 0 ) ||
                        ( receivedPacketReceiver.equals(nodeId) )
                       )
                    {
                        dispatchDataPacket(receivedPacket, topic);
                    }
                } else if ( type == RetransPacket.class )
                {
                    if ( sender[topic] == null )
                        return true;
                    if ( receivedPacketReceiver.equals(nodeId) ) {
                        dispatchRetransmissionRequest(receivedPacket, topic);
                    }
                } else if (type == ControlPacket.class )
                {
                    ControlPacket control = (ControlPacket) receivedPacket.cast();
                    if ( control.getType() == ControlPacket.DROPPED &&
                        receivedPacketReceiver.equals(nodeId) ) {
                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
                        if ( receiveBufferDispatcher != null ) {
                            FCLog.get().warn(nodeId+" has been dropped by "+receivedPacket.getSender()+" on service "+receiveBufferDispatcher.getTopicEntry().getTopicId());
                            FCSubscriber service = receiveBufferDispatcher.getTopicEntry().getSubscriber();
                            if ( service != null ) {
                                service.dropped();
                            }
//                            receiver[topic] = null;
                            // FIXME: initate resync
                        }
                    } else if ( control.getType() == ControlPacket.HEARTBEAT ) {
                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
                        if ( receiveBufferDispatcher != null ) {
                            Topic topicEntry = receiveBufferDispatcher.getTopicEntry();
                            topicEntry.registerHeartBeat(control.getSender().toString(), System.currentTimeMillis());
                        }
                    }
                }
            }
            return true;
        } 
        return false;
    }

    private void dispatchDataPacket(Packet receivedPacket, int topic) throws IOException {
        PacketReceiveBuffer buffer = receiver[topic].getBuffer(receivedPacket.getSender());
        DataPacket p = (DataPacket) receivedPacket.cast().detach();
        RetransPacket retransPacket = buffer.receivePacket(p);
        if ( retransPacket != null ) {
            // packet is valid just in this thread
            if ( PacketSendBuffer.RETRANSDEBUG )
                System.out.println("send retrans request " + retransPacket + " " + retransPacket.getClzId());
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
            FCLog.get().cluster("stopped receiving heartbeats from "+s);
            if ( receiveBufferDispatcher != null ) {
                receiveBufferDispatcher.cleanup(s);
            }
        }
    }

    public void subscribe( SubscriberConf subsConf, FCSubscriber subscriber ) {
        Topic topicEntry = topics.get(subsConf.getTopicId());
        if ( topicEntry == null )
            topicEntry = new Topic(null,null);
        if ( topicEntry.getPublisherConf() != null ) {
            throw new RuntimeException("already a sender registered at "+subsConf.getTopicId());
        }
        topicEntry.setReceiverConf(subsConf);
        topicEntry.setChannelDispatcher(this);
        topicEntry.setSubscriber(subscriber);
        installReceiver(topicEntry, subscriber);
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
}
