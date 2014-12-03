package org.nustaq.fastcast.control;

import org.nustaq.fastcast.packeting.TopicEntry;
import org.nustaq.fastcast.remoting.FCSubscriber;
import org.nustaq.fastcast.transport.Transport;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.fastcast.packeting.*;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/13/13
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class FCTransportDispatcher {

    public static final int IDLE_SPIN_IDLE_COUNT = 1000*1000;
    private static final int IDLE_SLEEP_MICRO_SECONDS = 1000;
    public static int MAX_NUM_TOPICS = 256;
    Transport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];
    long lastMsg[]; // marks last flush on a topic to enable batch delay

    StructString clusterName;
    StructString nodeId;

    Thread receiverThread, houseKeeping;
    FSTStructAllocator alloc = new FSTStructAllocator(1);

    public FCTransportDispatcher(Transport trans, String clusterName, String nodeId) {
        this.trans = trans;
        this.nodeId = alloc.newStruct( new StructString(nodeId) );
        this.clusterName = alloc.newStruct( new StructString(clusterName) );


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

    public void installReceiver(TopicEntry chan, FCSubscriber msgListener) {
        ReceiveBufferDispatcher receiveBufferDispatcher = new ReceiveBufferDispatcher(trans.getConf().getDgramsize(), clusterName.toString(), nodeId.toString(), chan, msgListener);
        if ( receiver[chan.getTopicId()] != null ) {
            throw new RuntimeException("double usage of topic "+chan.getTopicId()+" on transport "+trans.getConf().getName() );
        }
        receiver[chan.getTopicId()] = receiveBufferDispatcher;
    }

    public boolean hasReceiver(TopicEntry chan) {
        return receiver[chan.getTopicId()] != null;
    }

    public boolean hasSender(TopicEntry chan) {
        return sender[chan.getTopicId()] != null;
    }

    /**
     * installs and initializes sender thread and buffer, sets is to topicEntry given in argument !!
     * @param topicEntry
     */
    public PacketSendBuffer installSender(final TopicEntry topicEntry) {
        if ( sender[topicEntry.getTopicId()] != null ) {
            return sender[topicEntry.getTopicId()];
        }
        topicEntry.setTrans(trans);
        PacketSendBuffer packetSendBuffer = new PacketSendBuffer(trans, clusterName.toString(), nodeId.toString(), topicEntry);
        sender[topicEntry.getTopicId()] = packetSendBuffer;
        topicEntry.setSender(packetSendBuffer);
        return packetSendBuffer;
    }


    public void houseKeepingLoop() {
        while ( true ) {
            try {
                long now = System.currentTimeMillis();
                for (int i = 0; i < receiver.length; i++) {
                    ReceiveBufferDispatcher receiveBufferDispatcher = receiver[i];
                    if ( receiveBufferDispatcher != null ) {
                        TopicEntry topicEntry = receiveBufferDispatcher.getTopicEntry();
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
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    Packet receivedPacket;
    void receiveLoop()
    {
        byte[] receiveBuf = new byte[trans.getConf().getDgramsize()];
        DatagramPacket p = new DatagramPacket(receiveBuf,receiveBuf.length);
        receivedPacket = (Packet) alloc.newStruct(new Packet());
        int idleCount = 0;
        while(true) {
            try {
                if (receiveDatagram(p)) {
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

    private boolean receiveDatagram(DatagramPacket p) throws IOException {
        if ( trans.receive(p) ) {
            receivedPacket.baseOn(p.getData(), p.getOffset());

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
                    if ( true || // FIXME
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
                            TopicEntry topicEntry = receiveBufferDispatcher.getTopicEntry();
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

//    public void startListening(TopicEntry topic) {
//        installReceiver(topic, topic.getMsgReceiver() );
//    }
//
//    public void stopListening(TopicEntry topic) {
//        receiver[topic.getTopicId()] = null;
//    }

    public void cleanup(List<String> timedOutSenders, int topic) {
        for (int i = 0; i < timedOutSenders.size(); i++) {
            String s = timedOutSenders.get(i);
            ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
            FCLog.get().cluster("stopped receiving heartbeats from "+s);
            if ( receiveBufferDispatcher != null ) {
                receiveBufferDispatcher.cleanup(s);
            }
        }
    }
}
