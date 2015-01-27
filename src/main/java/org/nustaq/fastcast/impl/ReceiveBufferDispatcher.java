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

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.structs.structtypes.StructString;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 14.08.13
 * Time: 00:42
 * To change this template use File | Settings | File Templates.
 */

/**
 * multiplexes messages of different publishers to their
 * associated PacketReceiveBuffer
 */
public class ReceiveBufferDispatcher {

    ConcurrentHashMap<StructString,PacketReceiveBuffer> bufferMap = new ConcurrentHashMap<StructString, PacketReceiveBuffer>();

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
                topicEntry.getSubscriberConf().receiveBufferPackets(newHist);
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
            FCSubscriber subscriber = packetReceiveBuffer.getTopicEntry().getSubscriber();
            if ( subscriber != null ) {
                subscriber.senderTerminated(senderName);
            } else {
                FCLog.get().severe("no subscriber for stopped sender", null);
            }
            packetReceiveBuffer.terminate();
        }
        else {
            FCLog.get().warn("cannot find packetReceiver to terminate");
        }
    }

    public void cleanupTopic() {
        ArrayList<StructString> keys = new ArrayList(bufferMap.keySet());
        for (int i = 0; i < keys.size(); i++) {
            StructString o =  keys.get(i);
            cleanup(o.toString());
        }
    }

    public void getTimedOutSenders(long now, long timeout, List<String> res) {
        for (Iterator<PacketReceiveBuffer> iterator = bufferMap.values().iterator(); iterator.hasNext(); ) {
            PacketReceiveBuffer next = iterator.next();
            if ( now - next.getLastHBMillis() > timeout ) {
                System.out.println("timeout "+new Date(now)+" last:"+new Date(next.getLastHBMillis()));
                res.add(next.getReceivesFrom());
            }
        }
    }
}
