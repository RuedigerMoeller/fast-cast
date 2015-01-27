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

import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.malloc.MallocBytezAllocator;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructArray;
import org.nustaq.offheap.structs.structtypes.StructString;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/11/13
 * Time: 4:47 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * tracks packets and sequences sent from a single sender
 */
public class PacketReceiveBuffer {

    public static final int MAX_NON_GAP_PACKET_SERIES_TO_JUSTIFY_NEW_RETRANS_ENTRY = 20;
    final int topic;
    final int payMaxLen;
    final FSTStructAllocator packetAllocator;
    final StructArray<DataPacket> readBuffer;

    long maxOrderedSeq = 0; // highest ordered
    long highestSeq = 0; // highest sequence ever received

    String receivesFrom; // a receiveBuffer is responsible for a single sender only
    final StructString nodeId;
    FCSubscriber receiver;

    RetransPacket retrans; // used temporary from receive to return retrans packet

    Defragmenter decoder = new Defragmenter() {
        @Override
        public void msgDone(long seq, Bytez b, int off, int len) {
            if ( len == 1 && b.get(off) == ControlPacket.HEARTBEAT ) {
                // timestamp updated anyway
            } else {
                receiver.messageReceived(receivesFrom,seq,b,off,len);
            }
        }
    };

    private boolean isUnordered = false;
    private boolean isUnreliable = false;
    private volatile boolean terminated = false;
    int dGramSize;

    RetransPacket retransTemplate;
    DataPacket template;
    volatile long lastHBMillis;

    public PacketReceiveBuffer(int dataGramSizeBytes, String theNodeId, int historySize, String receivesFrom, Topic entry, FCSubscriber receiver) {
        topicEntry = entry;
        dGramSize = dataGramSizeBytes;
        this.topic = entry.getTopicId();
        this.receiver = receiver;
        template = DataPacket.getTemplate(dataGramSizeBytes);
        payMaxLen = template.data.length;

        template.getSender().setString(theNodeId);
        template.setTopic(topic);

        retransTemplate = new RetransPacket();
        retransTemplate.getSender().setString(theNodeId);
        retransTemplate.getReceiver().setString(receivesFrom);
        retransTemplate.setTopic(topic);
        retransTemplate.setSeqNo(-1);

        packetAllocator = new FSTStructAllocator(10, new MallocBytezAllocator());
        readBuffer = packetAllocator.newArray(historySize,template);
        this.nodeId = packetAllocator.newStruct( new StructString(Packet.MAX_NODE_NAME_LEN) );
        this.nodeId.setString(theNodeId);

        if ( readBuffer.getByteSize() > 5*1024*1024 ) {
            FCLog.log("allocating read buffer for topic '"+topicEntry.getTopicId()+"' of "+(readBuffer.getByteSize()/1024/1024)+" MByte");
        } else {
            FCLog.log("allocating read buffer for topic '"+topicEntry.getTopicId()+"' of "+(readBuffer.getByteSize()/1024)+" KByte");
        }
        retrans = packetAllocator.newStruct(retransTemplate);
        if ( dGramSize < retrans.getByteSize()+10 )
            throw new RuntimeException("datagram size must not be smaller than "+retrans.getByteSize()+10 );

        this.receivesFrom = receivesFrom;
        isUnordered = topicEntry.isUnordered();
        isUnreliable = topicEntry.isUnreliable();
        maxDelayRetrans = topicEntry.getSubscriberConf().getMaxDelayRetransMS();
        maxDelayNextRetrans = topicEntry.getSubscriberConf().getMaxDelayNextRetransMS();
    }

    public Topic getTopicEntry() {
        return topicEntry;
    }

    DataPacket getPacket(long seqNo) {
        return readBuffer.get((int) (seqNo%readBuffer.size()));
    }

    public long getMaxDelayNextRetrans() {
        return maxDelayNextRetrans;
    }

    public void setMaxDelayNextRetrans(long maxDelayNextRetrans) {
        this.maxDelayNextRetrans = maxDelayNextRetrans;
    }

    public long getMaxDelayRetrans() {
        return maxDelayRetrans;
    }

    public void setMaxDelayRetrans(long maxDelayRetrans) {
        this.maxDelayRetrans = maxDelayRetrans;
    }

    int retransCount = 0;
    long firstGapDetected = 0;
    long maxDelayNextRetrans = 15;
    long maxDelayRetrans = 0;
    boolean inInitialSync = true; // in case first packet is chained, stay in initial until complete msg is found
    Topic topicEntry;

    long startTime = 0;


    public RetransPacket receivePacket(DataPacket packet) {
        if (terminated)
            return null;
        updateHeartBeat(System.currentTimeMillis());
        if ( maxOrderedSeq == 0 ) {
            if ( startTime == 0 ) {
                startTime = System.currentTimeMillis();
//                return null;
            } else {
//                if ( System.currentTimeMillis()-startTime < 200 ) {
//                    System.out.println("initial dropping seq "+packet.getSeqNo()+" sender:"+packet.getSender()+" "+topicEntry.getConf().getName());
//                    return null;
//                }
            }
        }
        if ( isUnreliable ) {
            receivePacketUnreliable(packet);
            return null;
        } else if ( isUnordered ) {
            RetransPacket retransPacket = receivePacketUnOrdered(packet);
            return retransPacket;
        } else {
            RetransPacket retransPacket = receivePacketOrdered(packet);
            return retransPacket;
        }
    }

    public void receivePacketUnreliable(DataPacket packet) {
        long seqNo = packet.getSeqNo();
        int index = (int) (seqNo % readBuffer.size());
        highestSeq = Math.max(seqNo,highestSeq);

        if ( maxOrderedSeq == 0 ) {
            handleInitialSync(seqNo);
        }

        DataPacket previousPacket = getPacket(seqNo);
        if ( ! previousPacket.isDecoded() && previousPacket.getSeqNo() > 0 ) { // not decoded yet => drop packet
            return;
        }

        readBuffer.set(index,packet);
        DataPacket toDecode = readBuffer.get(index);
        decodePacket(toDecode);
    }

    private boolean isForeignPacket(DataPacket toDecode) {
        StructString rec = toDecode.getReceiver();
        return rec != null && rec.getLen() > 0 && ! nodeId.equals(rec);
    }

    public RetransPacket receivePacketUnOrdered(DataPacket packet) {
        RetransPacket toReturn = null;
        long seqNo = packet.getSeqNo();
        int index = (int) (seqNo % readBuffer.size());
        highestSeq = Math.max(seqNo,highestSeq);

        long now = System.currentTimeMillis(); // FIXME: not always needed !
        if ( seqNo != maxOrderedSeq+1 && firstGapDetected > 0 && now - firstGapDetected > maxDelayRetrans ) {
            // generate retransmission requests
            toReturn = computeRetransPacket(now);
        }

        if ( maxOrderedSeq == 0 ) {
            handleInitialSync(seqNo);
        }

        DataPacket previousPacket = getPacket(seqNo);
        if ( ! previousPacket.isDecoded() && previousPacket.getSeqNo() > 0 ) { // not decoded yet => drop packet
            return toReturn;
        }

        if ( seqNo == maxOrderedSeq+1 ) {
            // packet is next one
            readBuffer.set(index,packet);
            maxOrderedSeq = seqNo;
            DataPacket toDecode = readBuffer.get(index);
            decodePacket(toDecode);
            // if a gap was filled => deliver continous packets
            if ( ! inSync() ) { // there might be future packets in buffer
                DataPacket pack = getPacket(seqNo + 1);
                while ( pack.getSeqNo() == seqNo+1 ) {
                    // if unordered => check packages are not yet decoded
                    if ( ! pack.isDecoded() ) {
                        decodePacket(pack);
                    }
                    seqNo++;
                    maxOrderedSeq = seqNo;
                    pack = getPacket(seqNo + 1);
                }
//                System.out.println("done from loop highest:"+highestSeq);
                highestSeq = Math.max(maxOrderedSeq,highestSeq);
                if ( inSync() )
                {
                    if ( PacketSendBuffer.RETRANSDEBUG )
                        FCLog.get().net("**************** in sync");
                    firstGapDetected = 0;
                    retransCount = 0;
                    return toReturn;
                } else {
                    return toReturn;
                }
            } else {
                return toReturn; // in sync
            }
        } else {
            // gap,
            if ( firstGapDetected == 0 ) {
                firstGapDetected = now;
            }
            // if slot is free . deliver it
            if ( readBuffer.get(index).isDecoded() ) {
                readBuffer.set(index, packet);
                decodePacket(readBuffer.get(index));
            }
        }
        return toReturn;
    }

    long logBremse;
    public RetransPacket receivePacketOrdered(DataPacket packet) {
        if ( retransCount > 1 && PacketSendBuffer.RETRANSDEBUG ) {
            long now = System.currentTimeMillis();
            if ( now-logBremse > 1000 )
            {
                FCLog.get().warn("wait for retrans, received " + packet.getSeqNo() + " " + getTopicEntry().getSubscriberConf().getTopicId() + " waiting for " + (maxOrderedSeq + 1));
                if ( packet.getSeqNo() < maxOrderedSeq ) {
                    FCLog.get().warn("   sent by " + packet.getSender());
                }
                logBremse = now;
            }
        }
        RetransPacket toReturn = null;
        long seqNo = packet.getSeqNo();
        int index = (int) (seqNo % readBuffer.size());
        highestSeq = Math.max(seqNo,highestSeq);
        long now = System.currentTimeMillis(); // FIXME: not always needed !

        // if packet has been received and stored => return. Old packet or queue is full
        if ( seqNo <= maxOrderedSeq ) {
            return null;
        }

        if ( maxOrderedSeq == 0 ) {
            handleInitialSync(seqNo);
        }

        if ( seqNo == maxOrderedSeq+1 ) {
            // packet is next packet
            readBuffer.set(index,packet);
            maxOrderedSeq = seqNo;
            DataPacket toDecode = readBuffer.get(index);
            decodePacket(toDecode);

            retransCount = 0; // reset interval extension if any packet was received in sequence
            // if a gap was filled => deliver continous packets
            if ( ! inSync() ) { // there might be future packets in buffer
                boolean onePack = true;
                while( onePack ) {
                    onePack = false;
                    DataPacket pack = getPacket(seqNo + 1);
                    while ( pack.getSeqNo() == seqNo+1 ) {
//                        System.out.println("continue from buff "+(seqNo+1)+" "+pack.getSeqNo() );
                        decodePacket(pack);
                        seqNo++;
                        maxOrderedSeq = seqNo;
                        pack = getPacket(seqNo + 1);
                        onePack = true;
                    }
                }
                highestSeq = Math.max(maxOrderedSeq,highestSeq);
//                System.out.println("highest "+highestSeq);
                if ( inSync() )
                {
                    if ( PacketSendBuffer.RETRANSDEBUG )
                        FCLog.get().net("**************** in sync");
//                    System.out.println("INSYNC");
                    firstGapDetected = 0;
                    return null;//toReturn;
                } else {
                    if ( toReturn != null ) {
//                        System.out.println("returned retrans 1 "+toReturn);
                    }
                    return toReturn;
                }
            } else {
                return null; // in sync
            }
        } else {
            // gap detected
            if ( firstGapDetected == 0 ) {
                firstGapDetected = now;
                toReturn = computeRetransPacket(now);
            } else if ( firstGapDetected < now ) {
                toReturn = computeRetransPacket(now);
            }
        }

        // at this point it is sure packet is future packet
        // store future packet
        readBuffer.set(index, packet);
        return toReturn;
    }


    private void handleInitialSync(long seqNo) {
        maxOrderedSeq = seqNo-1; // ok, init only
        inInitialSync = true;
        FCLog.get().info("for sender "+receivesFrom+" bootstrap sequence "+getTopicEntry().getSubscriberConf().getTopicId()+" no "+seqNo);
        final FCSubscriber subscriber = getTopicEntry().getSubscriber();
        if ( subscriber != null ) {
            subscriber.senderBootstrapped(receivesFrom,seqNo);
        }
    }

    private RetransPacket computeRetransPacket(long now) {
        RetransPacket toReturn = (RetransPacket) retrans.createCopy();
        toReturn.clear();
        long curSeq = maxOrderedSeq+1;
        boolean anotherGapNearCurrentGap = false;
        while( curSeq < highestSeq && ! toReturn.isFull() ) {
            if ( getPacket(curSeq).getSeqNo() != curSeq ) {
                if ( ! anotherGapNearCurrentGap )
                    toReturn.current().setFrom(curSeq);
                curSeq++;
                while( curSeq < highestSeq && ! toReturn.isFull() && getPacket(curSeq).getSeqNo() != curSeq ) {
                    curSeq++;
                }
                anotherGapNearCurrentGap = false;
// fixme: at least keep < maxOrdered
//                for ( long off = curSeq; off < curSeq+MAX_NON_GAP_PACKET_SERIES_TO_JUSTIFY_NEW_RETRANS_ENTRY; off++ ) {
//                    if ( off < highestSeq && ! toReturn.isFull() && getPacket(off).getSeqNo() != off ) {
//                        anotherGapNearCurrentGap = true;
//                        curSeq = off;
//                        break;
//                    } else {
//
//                    }
//                }
                if ( ! anotherGapNearCurrentGap ) {
                    toReturn.current().setTo(curSeq);
                    toReturn.nextEntry();
                }
            } else {
                curSeq++;
            }
        }
        retransCount++;
        long delay = maxDelayNextRetrans * (1+(retransCount/5));
        if ( retransCount > 5 ) { // FIXME: give up at some point ?
            FCLog.get().warn("retransmission retrial at " + maxOrderedSeq + " count " + retransCount + " highest " + highestSeq + " stream " + getTopicEntry().getSubscriberConf().getTopicId()+" retrans:"+toReturn+" delay:"+ delay);
        }
        firstGapDetected = delay + now;
        return toReturn;
    }

    private boolean isUnordered() {
        return isUnordered;
    }

    public boolean inSync() {
        return highestSeq == maxOrderedSeq;
    }

    FSTStruct currentPacketBytePointer = new FSTStruct();

    long debugPrevSeq = 0;
    long lastPacket = 0;
    FSTStruct tmpStruct = new FSTStruct();
    DataPacket tmpPacket;
    void decodePacket(DataPacket packet) {
//        packet.dumpBytes();

        if ( receiver == null )
            return;

        if ( isForeignPacket(packet) ) {
            return;
        } else
        {
//            System.out.println("received "+packet.getReceiver());
        }

        final long packetSeqNo = packet.getSeqNo();

        if ( tmpPacket == null ) {
            tmpPacket = packetAllocator.newPointer(DataPacket.class); // fixme: move to init
        }

        packet.dataPointer(tmpStruct);
        final Bytez dataPacketBase = tmpStruct.getBase();
        final int dataindex = (int) tmpStruct.getOffset();
        final int packIndex = (int) packet.getOffset();

        decodeMsgBytes(packetSeqNo, dataPacketBase, dataindex, packIndex);
    }

    private void decodeMsgBytes(long packetSeqNo, Bytez dataPacketBase, int dataindex, int packIndex) {

//        check is valid only for non unicast traffic (else packets for other nodes will not be sent and create pseudo sequence gaps
//        if (!isUnordered() && !isUnreliable() && debugPrevSeq != 0 && debugPrevSeq != packetSeqNo - 1) {
//            FCLog.get().fatal("FATAL ERROR " + packetSeqNo);
//            System.exit(1);
//        }

        debugPrevSeq = packetSeqNo;

        currentPacketBytePointer.baseOn(dataPacketBase, dataindex);
        while (true) {
            short code = currentPacketBytePointer.getShort();
            if (code > DataPacket.MAX_CODE || code < 0) {
                FCLog.get().warn("foreign traffic or error, maxOrdered " + maxOrderedSeq + " packseq " + packetSeqNo + " highest " + highestSeq);
                System.exit(1);
            }
            currentPacketBytePointer.next(2);
            if (code == DataPacket.EOP) {
                if (isUnordered()||isUnreliable()) {
                    tmpPacket.baseOn(dataPacketBase, packIndex);
                    tmpPacket.setDecoded(true);
                }
                return;
            } else {
                short len = currentPacketBytePointer.getShort();
                currentPacketBytePointer.next(2);
                if (inInitialSync) {
                    if (code == DataPacket.COMPLETE) {
                        inInitialSync = false;
                    }
                } else {
                    decoder.receiveChunk(packetSeqNo, currentPacketBytePointer.getBase(), (int) currentPacketBytePointer.getOffset(), len, code == DataPacket.COMPLETE);
                }
                currentPacketBytePointer.next(len);
            }
        }
    }

    public void setUnreliable(boolean unreliable) {
        isUnreliable = unreliable;
    }

    public boolean isUnreliable() {
        return isUnreliable;
    }

    public void terminate() {
        terminated = true;
        // avoid late packets to crash system
        new Thread("freedom") {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                freeImmediate();
            }
        }.start();
    }

    private void freeImmediate() {
        long alloced = MallocBytezAllocator.alloced.get();
        packetAllocator.free();
        long curr = MallocBytezAllocator.alloced.get();
        FCLog.log("freed " + (alloced - curr) / 1024 / 1024 + "MB to " + curr / 1024 / 1024 + " MB");
    }

    /**
     * reset all sequences and resync (creates unrecoverable message loss)
     */
    public void resync() {
        maxOrderedSeq = 0;
        startTime = 0;
        retransCount = 0;
        firstGapDetected = 0;
        debugPrevSeq = 0;
        inInitialSync = true; // in case first packet is chained, stay in initial until complete msg is found
    }

    public void updateHeartBeat(long l) {
        lastHBMillis = l;
    }

    public long getLastHBMillis() {
        return lastHBMillis;
    }

    public String getReceivesFrom() {
        return receivesFrom;
    }
}
