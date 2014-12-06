package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.structs.structtypes.StructString;

import java.util.ArrayList;
import java.util.HashMap;

/*
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 14.08.13
 * Time: 00:42
 * To change this template use File | Settings | File Templates.
 */

/**
 * receives messages of a topic and multiplexes messages of different publishers to their
 * associated PacketReceiveBuffer
 */
public class ReceiveBufferDispatcher {

    HashMap<StructString,PacketReceiveBuffer> bufferMap = new HashMap<StructString, PacketReceiveBuffer>();

    int packetSize;
    String nodeId;
    int historySize;
    int topic;
    FCSubscriber receiver;
    Topic topicEntry;

    public ReceiveBufferDispatcher(int packetSize, String nodeId, Topic entry, FCSubscriber rec) {
        receiver = rec;
        this.packetSize = packetSize;
        this.nodeId = nodeId;
        this.historySize = entry.getSubscriberConf().getReceiveBufferPackets();
        this.topic = entry.getTopicId();
        topicEntry = entry;
    }

    public Topic getTopicEntry() {
        return topicEntry;
    }

    public PacketReceiveBuffer getBuffer(StructString sender) {
        PacketReceiveBuffer receiveBuffer = bufferMap.get(sender);
        if ( receiveBuffer == null ) {
            int hSize = historySize;
            if ( ((long)hSize*packetSize) > Integer.MAX_VALUE-2*packetSize ) {
                final int newHist = (Integer.MAX_VALUE - 2 * packetSize) / packetSize;
                topicEntry.getSubscriberConf().setReceiveBufferPackets(newHist);
                FCLog.get().warn("int overflow, degrading history size from "+hSize+" to "+newHist);
                historySize = newHist;
            }
            receiveBuffer = new PacketReceiveBuffer(packetSize,nodeId,historySize,sender.toString(), topicEntry, receiver);
            bufferMap.put((StructString) sender.createCopy(),receiveBuffer);
        }
        return receiveBuffer;
    }

    /**
     * if a sender stops sending, remove from map to free memory
     * @param senderName
     */
    public void cleanup(String senderName) {
        StructString struct = new StructString(senderName);
        PacketReceiveBuffer packetReceiveBuffer = bufferMap.get(struct);
        bufferMap.remove(struct);
        if ( packetReceiveBuffer != null ) {
            packetReceiveBuffer.terminate();
            FCSubscriber subscriber = packetReceiveBuffer.getTopicEntry().getSubscriber();
            if ( subscriber != null ) {
                subscriber.senderTerminated(senderName);
            }
        }
        else {
            System.out.println("cannot find packetReceiver to terminate");
        }
    }

    public void cleanupTopic() {
        ArrayList<String> keys = new ArrayList(bufferMap.keySet());
        for (int i = 0; i < keys.size(); i++) {
            String o =  keys.get(i);
            cleanup(o);
        }
    }
}
