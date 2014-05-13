package de.ruedigermoeller.fastcast.control;

import de.ruedigermoeller.fastcast.config.FCTopicConf;
import de.ruedigermoeller.fastcast.packeting.TopicEntry;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.transport.Transport;
import de.ruedigermoeller.fastcast.packeting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;
import de.ruedigermoeller.heapoff.structs.FSTStructAllocator;
import de.ruedigermoeller.heapoff.structs.structtypes.StructString;
import de.ruedigermoeller.kontraktor.Actors;
import de.ruedigermoeller.kontraktor.impl.DispatcherThread;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/13/13
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 *
 * listens and sends
 *
 *
 */
public class FCTransportDispatcher {

    public static final int IDLE_SPIN_LOCK_PARK_NANOS = 30*1000;
    public static final int IDLE_SPIN_IDLE_COUNT = 1000*10;
    public static int MAX_NUM_TOPICS = 256;
    Transport trans;

    ReceiveBufferDispatcher receiver[];
    PacketSendBuffer sender[];

    StructString clusterName;
    StructString nodeId;

    Thread calbackCleaner;
    TransportReceiveActor receiverActor;
    FSTStructAllocator alloc = new FSTStructAllocator(1);

    public FCTransportDispatcher(Transport trans, String clusterNameString, String nodeIdString) {

        this.trans = trans;
        this.nodeId = (StructString) alloc.newStruct( new StructString(nodeIdString) );
        this.clusterName = (StructString) alloc.newStruct( new StructString(clusterNameString) );


        receiver = new ReceiveBufferDispatcher[MAX_NUM_TOPICS];
        sender = new PacketSendBuffer[MAX_NUM_TOPICS];

        receiverActor = Actors.SpawnActor(TransportReceiveActor.class);
        receiverActor.getDispatcher().setName("Receiver Actor "+trans.getConf().getName());
        receiverActor.init(trans, this.clusterName, this.nodeId, receiver, sender);

        Thread senderThread = new Thread("trans sender "+trans.getConf().getName()) {
            public void run() {
                sendLoop();
            }
        };
        senderThread.start();

        calbackCleaner = new Thread("callback cleaner") {
            public void run() {
                cleanCBLoop();
            }

        };
        calbackCleaner.start();
        receiverActor.receiveLoop();
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
     * installs and initializes sender thread and buffer, sets is to topicEntry given in argument !!
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
        }
    }

    public PacketSendBuffer getSender(int topic) {
        return sender[topic];
    }

    Bytez heartbeat = new HeapBytez(new byte[]{FastCast.HEARTBEAT});

    private boolean sendStep(int topic) {
        if ( trans == null || sender[topic] == null ) {
            return false;
        }
        boolean anyThingSent = false;
        try {
            PacketSendBuffer packetSendBuffer = sender[topic];
            anyThingSent = packetSendBuffer.send(trans);
        } catch (Exception e) {
            FCLog.log(e);
        }
        return anyThingSent;
    }

    private void sendLoop() {
        int idleCount = 0;
        int iterationCount = 0;
        long lastStat = System.currentTimeMillis();
        long lastHB = System.currentTimeMillis();
        while( true ) {
            int count = 0;
            for (int i = 0; i < sender.length; i++) {
                if (sendStep(i))
                    count++;
            }
            if (count == 0) {
                idleCount++;
            } else {
                idleCount = 0;
            }
            iterationCount++;
            if ( count % 100 == 0 ) {
                long now = System.currentTimeMillis();
                for (int i = 0; i < sender.length; i++) {
                    PacketSendBuffer packetSendBuffer = sender[i];
                    if ( packetSendBuffer != null ) {
                        if (now - lastStat > FCTopicConf.flowControlInterval) {
                            packetSendBuffer.doFlowControl();
                            lastStat = now;
                        }
                        if (now - lastHB > FCTopicConf.heartbeatInterval) {
                            boolean succ = putHeartbeat(packetSendBuffer);
                            if (succ)
                                lastHB = now;
                        }
                    }
                }
            }
            DispatcherThread.yield(idleCount);
        }
    }

    public boolean putHeartbeat(PacketSendBuffer packetSendBuffer) {
        return packetSendBuffer.putMessage(-1,heartbeat,0,1,true);
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
