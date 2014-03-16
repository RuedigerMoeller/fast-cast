package de.ruedigermoeller.fastcast.control;

import de.ruedigermoeller.fastcast.packeting.TopicEntry;
import de.ruedigermoeller.fastcast.remoting.FCRemotingListener;
import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.transport.Transport;
import de.ruedigermoeller.fastcast.packeting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;
import de.ruedigermoeller.heapoff.structs.FSTStructAllocator;
import de.ruedigermoeller.heapoff.structs.structtypes.StructString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/13/13
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class FCTransportDispatcher {

    public static int MAX_NUM_TOPICS = 256;
    Transport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];

    StructString clusterName;
    StructString nodeId;

    Thread receiverThread, calbackCleaner;
    FSTStructAllocator alloc = new FSTStructAllocator(1);

    public FCTransportDispatcher(Transport trans, String clusterName, String nodeId) {
        this.trans = trans;
        this.nodeId = (StructString) alloc.newStruct( new StructString(nodeId) );
        this.clusterName = (StructString) alloc.newStruct( new StructString(clusterName) );


        receiver = new ReceiveBufferDispatcher[MAX_NUM_TOPICS];
        sender = new PacketSendBuffer[MAX_NUM_TOPICS];

        receiverThread = new Thread("trans receiver "+trans.getConf().getName()) {
            public void run() {
                receiveLoop();
            }
        };

        calbackCleaner = new Thread("callback cleaner") {
            public void run() {
                cleanCBLoop();
            }

        };
        receiverThread.start();
        calbackCleaner.start();
    }

    void cleanCBLoop() {
        while( true ) {
            try {
                Thread.sleep(100);
                long now = System.currentTimeMillis();
                for (int i = 0; i < sender.length; i++) {
                    PacketSendBuffer packetSendBuffer = sender[i];
                    if ( packetSendBuffer != null ) {
                        packetSendBuffer.getTopicEntry().getCbMap().release(now);
                    }
                }
            } catch (Exception e) {
                FCLog.log(e);
            }
        }
    }

    public void installReceiver(TopicEntry chan, MsgReceiver msgListener) {
        ReceiveBufferDispatcher receiveBufferDispatcher = new ReceiveBufferDispatcher(trans.getConf().getDgramsize(), clusterName.toString(), nodeId.toString(), chan, msgListener);
        if ( receiver[chan.getTopic()] != null ) {
            throw new RuntimeException("double usage of topic "+chan.getTopic()+" on transport "+trans.getConf().getName() );
        }
        receiver[chan.getTopic()] = receiveBufferDispatcher;
    }

    public boolean hasReceiver(TopicEntry chan) {
        return receiver[chan.getTopic()] != null;
    }

    public boolean hasSender(TopicEntry chan) {
        return sender[chan.getTopic()] != null;
    }

    /**
     * installs & initializes sender thread anmd buffer, sets is to topicEntry given in argunment !!
     * @param topicEntry
     */
    public void installSender(final TopicEntry topicEntry) {
        if ( sender[topicEntry.getTopic()] != null ) {
            return;
        }
        topicEntry.setTrans(trans);
        PacketSendBuffer packetSendBuffer = new PacketSendBuffer(trans.getConf().getDgramsize(), clusterName.toString(), nodeId.toString(), topicEntry);
        sender[topicEntry.getTopic()] = packetSendBuffer;
        topicEntry.setSender(packetSendBuffer);
        if ( topicEntry.getConf().getMaxSendPacketQueueSize() == 0 ) // no sender thread
        {
        } else {
            Thread senderThread = new Thread("trans sender "+trans.getConf().getName()+" "+topicEntry.getName()) {
                public void run() {
                    sendLoop(topicEntry);
                }
            };
            senderThread.start();
        }
    }

    public PacketSendBuffer getSender(int topic) {
        return sender[topic];
    }

    Bytez heartbeat = new HeapBytez(new byte[]{FastCast.HEARTBEAT});

    private void sendLoop(TopicEntry topic) {
        while( trans == null || sender[topic.getTopic()] == null ) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                FCLog.log(e);
            }
        }
        PacketSendBuffer packetSendBuffer = sender[topic.getTopic()];
        long lastStat = System.currentTimeMillis();
        long lastHB = System.currentTimeMillis();
        int count = 0;
        long flowControlInterval = topic.getConf().getFlowControlInterval();
        long heartBeatInterval = topic.getConf().getHeartbeatInterval();
        int nothingSentCount = 0;
        while(true) {
            try {
                boolean anyThingSent = packetSendBuffer.send(trans);
                if ( anyThingSent ) {
                    nothingSentCount = 0;
                } else {
                    nothingSentCount++;
                }
                if ( !packetSendBuffer.useSpinLock() && nothingSentCount > 1000 ) {
                    Object sendWakeupLock = packetSendBuffer.getSendWakeupLock();
                    nothingSentCount = 0;
                    synchronized (sendWakeupLock) {
                        sendWakeupLock.wait(0,1000);
                    }
                }
                count++;
                if ( count % 100 == 0 ) {
                    long now = System.currentTimeMillis();
                    if ( now - lastStat > flowControlInterval ) {
                        packetSendBuffer.doFlowControl();
                        lastStat = now;
                    }
                    if ( now - lastHB > heartBeatInterval ) {
                        boolean succ = putHeartbeat(packetSendBuffer);
                        if ( succ )
                            lastHB = now;
                    }
                }
            } catch (Exception e) {
                FCLog.log(e);
            }
        }
    }

    public boolean putHeartbeat(PacketSendBuffer packetSendBuffer) {
        return packetSendBuffer.putMessage(-1,heartbeat,0,1,true);
    }

    Packet receivedPacket;
    void receiveLoop()
    {
        byte[] receiveBuf = new byte[trans.getConf().getDgramsize()];
        DatagramPacket p = new DatagramPacket(receiveBuf,receiveBuf.length);
        receivedPacket = (Packet) alloc.newStruct(new Packet());
        while(true) {
            try {
                receiveDatagram(p);
            } catch (IOException e) {
                FCLog.log(e);
            }
        }
    }

    private void receiveDatagram(DatagramPacket p) throws IOException {
        if ( trans.receive(p) ) {
            receivedPacket.baseOn(p.getData(), p.getOffset());

            boolean sameCluster = receivedPacket.getCluster().equals(clusterName);
            boolean selfSent = receivedPacket.getSender().equals(nodeId);

            if ( sameCluster && ! selfSent) {

                int topic = receivedPacket.getTopic();

                if ( topic > MAX_NUM_TOPICS || topic < 0 ) {
                    FCLog.get().warn("foreign traffic");
                    return;
                }
                if ( receiver[topic] == null && sender[topic] == null) {
                    return;
                }

                Class type = receivedPacket.getPointedClass();
                StructString receivedPacketReceiver = receivedPacket.getReceiver();
                if ( type == DataPacket.class )
                {
                    if ( receiver[topic] == null )
                        return;
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
                        return;
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
                            FCLog.get().warn(nodeId+" has been dropped by "+receivedPacket.getSender()+" on service "+receiveBufferDispatcher.getTopicEntry().getName());
                            FCTopicService service = receiveBufferDispatcher.getTopicEntry().getService();
                            if ( service != null ) {
                                service.droppedFromReceiving();
                            }
                            FCRemotingListener remotingListener = FastCast.getRemoting().getRemotingListener();
                            if ( remotingListener != null ) {
                                remotingListener.droppedFromTopic(receiveBufferDispatcher.getTopicEntry().getTopic(),receiveBufferDispatcher.getTopicEntry().getName());
                            }
                            receiver[topic] = null;
                        }
                    }
                }
            }
        }
    }

    private void dispatchDataPacket(Packet receivedPacket, int topic) throws IOException {
        PacketReceiveBuffer buffer = receiver[topic].getBuffer(receivedPacket.getSender());
        DataPacket p = (DataPacket) receivedPacket.cast().detach();
        RetransPacket retransPacket = buffer.receivePacket(p);
        if ( retransPacket != null ) {
            // packet is valid just in this thread
            if ( PacketSendBuffer.RETRANSDEBUG )
                System.out.println("send retrans request " + retransPacket + " " + retransPacket.getClzId());
            trans.send(new DatagramPacket(retransPacket.getBase().toBytes((int) retransPacket.getOffset(), retransPacket.getByteSize()), 0, retransPacket.getByteSize()));
        }
    }

    private void dispatchRetransmissionRequest(Packet receivedPacket, int topic) throws IOException {
        RetransPacket retransPacket = (RetransPacket) receivedPacket.cast().detach();
        sender[topic].addRetransmissionRequest(retransPacket, trans);
    }

    public void startListening(TopicEntry topic) {
        installReceiver(topic, topic.getMsgReceiver() );
    }

    public void stopListening(TopicEntry topic) {
        receiver[topic.getTopic()] = null;
    }

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
