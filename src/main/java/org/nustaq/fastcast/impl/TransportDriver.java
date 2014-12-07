package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.api.FCPublisher;
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

    int spinIdleLoopMax = 1000*1000*10;
    int idleParkMicros = 1000;

    PhysicalTransport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];
    long lastMsg[]; // marks last flush on a topic to enable batch delay

    StructString nodeId;

    Thread receiverThread, houseKeeping;
    FSTStructAllocator alloc = new FSTStructAllocator(1);
    long autoFlushMS;
    private ConcurrentHashMap<Integer,Topic> topics = new ConcurrentHashMap<>();

    public TransportDriver(PhysicalTransport trans, String nodeId) {
        this.trans = trans;
        this.nodeId = alloc.newStruct( new StructString(nodeId) );
        final PhysicalTransportConf tconf = trans.getConf();
        this.autoFlushMS = tconf.getAutoFlushMS();
        this.spinIdleLoopMax = tconf.getSpinIdleLoopMax();
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

    private void houseKeepingLoop() {
        ArrayList<String> lostSenders = new ArrayList<>();
        while ( true ) {
            try {
                long now = System.currentTimeMillis();
                for (int i = 0; i < receiver.length; i++) {
                    ReceiveBufferDispatcher receiveBufferDispatcher = receiver[i];
                    if ( receiveBufferDispatcher != null ) {
                        Topic topicEntry = receiveBufferDispatcher.getTopicEntry();
                        lostSenders.clear();
                        List<String> timedOutSenders = topicEntry.getTimedOutSenders(lostSenders,now, topicEntry.getHbTimeoutMS());
                        if ( timedOutSenders != null && timedOutSenders.size() > 0 ) {
                            cleanup(timedOutSenders, i);
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
                    if ( idleCount > spinIdleLoopMax) {
                        LockSupport.parkNanos(1000*idleParkMicros);
                    }
                }
            } catch (IOException e) {
                FCLog.log(e);
            }
        }
    }

    private boolean receiveDatagram(ByteBuffer p, byte wrappedArr[]) throws IOException {
        if ( trans.receive(p) ) {

            boolean selfSent = receivedPacket.getSender().equals(nodeId);

            if ( ! selfSent) {

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
            }
            return true;
        } 
        return false;
    }

    private boolean isForeignReceiver(StructString receivedPacketReceiver) {
        return receivedPacketReceiver != null && receivedPacketReceiver.getLen() > 0 && ! receivedPacketReceiver.equals(nodeId);
    }

    private void dispatchDataPacket(Packet receivedPacket, int topic) throws IOException {
        PacketReceiveBuffer buffer = receiver[topic].getBuffer(receivedPacket.getSender());
        DataPacket p = receivedPacket.cast(); //.detach(); // FIXME: alloc
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
        topicEntry.setSubscriberConf(subsConf);
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

    public ReceiveBufferDispatcher getReceiver(int topicId) {
        return receiver[topicId];
    }
}
