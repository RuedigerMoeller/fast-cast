package org.nustaq.fastcast.packeting;

import org.nustaq.fastcast.remoting.*;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.malloc.MallocBytezAllocator;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructArray;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/11/13
 * Time: 4:47 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * tracks packets sent from a single sender (single threaded)
 */
public class PacketReceiveBuffer {

    final int topic;
    final int payMaxLen;
    final FSTStructAllocator packetAllocator;
    final StructArray<DataPacket> readBuffer;

    AtomicLong maxOrderedSeq = new AtomicLong(0); // highest ordered
    AtomicLong maxDeliveredSeq = new AtomicLong(0);
    long highestSeq = 0; // highest sequence ever received

    String receivesFrom; // a receiveBuffer is responsible for a single sender only
    FCSubscriber receiver;

    RetransPacket retrans; // used temporary from receive to return retrans packet

    SimpleByteArrayReceiver decoder = new SimpleByteArrayReceiver() {
        @Override
        public void msgDone(long seq, Bytez b, int off, int len) {
            if ( len == 1 && b.get(off) == FastCast.HEARTBEAT ) { // FIXME: use controlpacket for heartbeats
                topicEntry.registerHeartBeat(receivesFrom,System.currentTimeMillis());
            } else if ( receiver != null ) {
                receiver.messageReceived(receivesFrom,seq,b,off,len);
            }
        }
    };

    private boolean isUnordered = false;
    private boolean isUnreliable = false;
    TopicStats stats;
    int lastOrderedSendPause;
    private boolean terminated = false;
    int dGramSize;
    static int recMatchCount;
    RetransPacket retransTemplate;
    DataPacket template;

    public PacketReceiveBuffer(int dataGramSizeBytes, String clusterName, String nodeId, int historySize, String receivesFrom, TopicEntry entry, FCSubscriber receiver) {
        topicEntry = entry;
        dGramSize = dataGramSizeBytes;
        this.topic = entry.getTopicId();
        this.receiver = receiver;
        template = DataPacket.getTemplate(dataGramSizeBytes);
        payMaxLen = template.data.length;

        template.getCluster().setString(clusterName);
        template.getSender().setString(nodeId);
        template.setTopic(topic);

        retransTemplate = new RetransPacket();
        retransTemplate.getCluster().setString(clusterName);
        retransTemplate.getSender().setString(nodeId);
        retransTemplate.getReceiver().setString(receivesFrom);
        retransTemplate.setTopic(topic);
        retransTemplate.setSeqNo(-1);

        packetAllocator = new FSTStructAllocator(10, new MallocBytezAllocator());
        readBuffer = packetAllocator.newArray(historySize,template);
        if ( readBuffer.getByteSize() > 5*1024*1024 ) {
            FCLog.log("allocating read buffer for topic '"+topicEntry.getTopicId()+"' of "+(readBuffer.getByteSize()/1024/1024)+" MByte");
        } else {
            FCLog.log("allocating read buffer for topic '"+topicEntry.getTopicId()+"' of "+(readBuffer.getByteSize()/1024)+" KByte");
        }
        retrans = packetAllocator.newStruct(retransTemplate);

        this.receivesFrom = receivesFrom;
        stats = topicEntry.getStats();
        isUnordered = topicEntry.isUnordered();
        isUnreliable = topicEntry.isUnreliable();
        maxDelayRetrans = topicEntry.getReceiverConf().getMaxDelayRetransMS();
        maxDelayNextRetrans = topicEntry.getReceiverConf().getMaxDelayNextRetransMS();
    }

    public TopicEntry getTopicEntry() {
        return topicEntry;
    }

    DataPacket getPacketVolatile( long seqNo ) {
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
    long maxDelayNextRetrans = 50;
    long maxDelayRetrans = 10;
    boolean inInitialSync = true; // in case first packet is chained, stay in initial until complete msg is found
    TopicEntry topicEntry;

    long startTime = 0;


    public RetransPacket receivePacket(DataPacket packet) {
        stats.dataPacketReceived(packet.getDGramSize());
        if ( maxOrderedSeq.get() == 0 ) {
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
            if ( retransPacket != null ) {
                stats.retransRQSent(retransPacket.computeNumPackets());
            }
            return retransPacket;
        } else {
            RetransPacket retransPacket = receivePacketOrdered(packet);
            if ( retransPacket != null ) {
                stats.retransRQSent(retransPacket.computeNumPackets());
            }
            return retransPacket;
        }
    }

    public void receivePacketUnreliable(DataPacket packet) {
        long seqNo = packet.getSeqNo();
        int index = (int) (seqNo % readBuffer.size());
        highestSeq = Math.max(seqNo,highestSeq);

        long now = System.currentTimeMillis(); // FIXME: not always needed !

        if ( maxOrderedSeq.get() == 0 ) {
            handleInitialSync(seqNo);
        }

        DataPacket previousPacket = getPacketVolatile(seqNo);
        if ( ! previousPacket.isDecoded() && previousPacket.getSeqNo() > 0 ) { // not decoded yet => drop packet
            return;
        }

        readBuffer.set(index,packet);
        DataPacket toDecode = readBuffer.get(index);
        decodePacket(toDecode);
    }

    public RetransPacket receivePacketUnOrdered(DataPacket packet) {
        RetransPacket toReturn = null;
        long seqNo = packet.getSeqNo();
        int index = (int) (seqNo % readBuffer.size());
        highestSeq = Math.max(seqNo,highestSeq);

        long now = System.currentTimeMillis(); // FIXME: not always needed !
        if ( seqNo != maxOrderedSeq.get()+1 && firstGapDetected > 0 && now - firstGapDetected > maxDelayRetrans ) {
            // generate retransmission requests
            toReturn = computeRetransPacket(now);
        }

        if ( maxOrderedSeq.get() == 0 ) {
            handleInitialSync(seqNo);
        }

        DataPacket previousPacket = getPacketVolatile(seqNo);
        if ( ! previousPacket.isDecoded() && previousPacket.getSeqNo() > 0 ) { // not decoded yet => drop packet
            return toReturn;
        }

        if ( seqNo == maxOrderedSeq.get()+1 ) {
            // packet is next one
            readBuffer.set(index,packet);
            maxOrderedSeq.set(seqNo);
            DataPacket toDecode = readBuffer.get(index);
            int sendPauseSender = toDecode.getSendPauseSender();
            if (sendPauseSender>0)
                lastOrderedSendPause = sendPauseSender;
            decodePacket(toDecode);
            // if a gap was filled => deliver continous packets
            if ( ! inSync() ) { // there might be future packets in buffer
                DataPacket pack = getPacketVolatile(seqNo+1);
                while ( pack.getSeqNo() == seqNo+1 ) {
                    // if unordered => check packages are not yet decoded
                    if ( ! pack.isDecoded() ) {
                        decodePacket(pack);
                    }
                    seqNo++;
                    maxOrderedSeq.set(seqNo);
                    pack = getPacketVolatile(seqNo+1);
                }
//                System.out.println("done from loop highest:"+highestSeq);
                highestSeq = Math.max(maxOrderedSeq.get(),highestSeq);
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
    // single threaded per sender
    public RetransPacket receivePacketOrdered(DataPacket packet) {
        if ( retransCount > 1 ) {
            long now = System.currentTimeMillis();
            if ( now-logBremse > 1000 )
            {
                System.out.println("wait for retrans, received "+packet.getSeqNo()+" "+getTopicEntry().getReceiverConf().getTopicId()+" waiting for "+(maxOrderedSeq.get()+1));
                if ( packet.getSeqNo() < maxOrderedSeq.get() ) {
                    System.out.println("   sent by "+packet.getSender());
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
        if ( seqNo <= maxOrderedSeq.get() ) {
            return null;
        }

        if ( maxOrderedSeq.get() == 0 ) {
            handleInitialSync(seqNo);
        }

        if ( seqNo != maxOrderedSeq.get()+1 && firstGapDetected > 0 && now - firstGapDetected >= maxDelayRetrans ) {
            toReturn = computeRetransPacket(now);
        }

        // queue full ?
        if ( maxOrderedSeq.get()-maxDeliveredSeq.get() > readBuffer.size()-3 ) {
            return toReturn;
        }

        if ( seqNo == maxOrderedSeq.get()+1 ) {
            // packet is next packet
            readBuffer.set(index,packet);
            maxOrderedSeq.set(seqNo);
            DataPacket toDecode = readBuffer.get(index);
            int sendPauseSender = toDecode.getSendPauseSender();
            if (sendPauseSender>0)
                lastOrderedSendPause = sendPauseSender;
            decodePacket(toDecode);

            retransCount = 0; // reset interval extension if any packet was received in sequence
            // if a gap was filled => deliver continous packets
            if ( ! inSync() ) { // there might be future packets in buffer
                boolean onePack = true;
                while( onePack ) {
                    onePack = false;
                    DataPacket pack = getPacketVolatile(seqNo+1);
                    while ( pack.getSeqNo() == seqNo+1 ) {
//                        System.out.println("continue from buff "+(seqNo+1)+" "+pack.getSeqNo() );
                        decodePacket(pack);
                        seqNo++;
                        maxOrderedSeq.set(seqNo);
                        pack = getPacketVolatile(seqNo+1);
                        onePack = true;
                    }
                }
                highestSeq = Math.max(maxOrderedSeq.get(),highestSeq);
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
            }
        }

        // at this point it is sure packet is future packet

        // if previously stored message is delivered => store future packet
        long prevSeq = readBuffer.get(index).getSeqNo();
        if ( prevSeq < maxDeliveredSeq.get() ) {
            readBuffer.set(index, packet);
        }
        else {
        }
        if ( toReturn != null ) {
//            System.out.println("returned retrans 2 "+toReturn);
        }
        return toReturn;
    }


    private void handleInitialSync(long seqNo) {
        maxOrderedSeq.set(seqNo-1); // ok, init only
        maxDeliveredSeq.set(seqNo-1); // ok, init only
        inInitialSync = true;
        FCLog.get().cluster("for sender "+receivesFrom+" bootstrap sequence "+getTopicEntry().getReceiverConf().getTopicId()+" no "+seqNo);
        final FCSubscriber subscriber = getTopicEntry().getSubscriber();
        if ( subscriber != null ) {
            subscriber.senderBootstrapped(receivesFrom,seqNo);
        }
    }

    private RetransPacket computeRetransPacket(long now) {
        RetransPacket toReturn = retrans;
//        RetransPacket toReturn = (RetransPacket) retrans.createCopy();
        toReturn.clear();
        toReturn.setSent(System.nanoTime());
        long curSeq = maxOrderedSeq.get()+1;
        while( curSeq < highestSeq && ! toReturn.isFull() ) {
            if ( getPacketVolatile(curSeq).getSeqNo() != curSeq ) {
                toReturn.current().setFrom(curSeq);
                curSeq++;
                while( curSeq < highestSeq && ! toReturn.isFull() && getPacketVolatile(curSeq).getSeqNo() != curSeq ) {
                    curSeq++;
                }
                toReturn.current().setTo(curSeq);
//                toReturn.current().setTo(highestSeq);
                toReturn.nextEntry();
//                break;
            } else {
                curSeq++;
            }
        }
        retransCount++;
        if ( retransCount > 10 ) { // FIXME: give up at some point ?
            FCLog.get().warn("retransmission retrial at " + maxOrderedSeq + " count " + retransCount + " highest " + highestSeq + " stream " + getTopicEntry().getReceiverConf().getTopicId()+" retrans:"+retrans);
        }
        firstGapDetected = maxDelayNextRetrans*Math.max(retransCount,100) + now;
        toReturn.setSendPauseSender(lastOrderedSendPause);
        return toReturn;
    }

    private boolean isUnordered() {
        return isUnordered;
    }

    public boolean inSync() {
        return highestSeq == maxOrderedSeq.get();
    }

    FSTStruct currentPacketBytePointer = new FSTStruct();

    long debugPrevSeq = 0;
    long lastPacket = 0;
    int packCount = 0;
    FSTStruct tmpStruct = new FSTStruct();
    DataPacket tmpPacket;
    void decodePacket(DataPacket packet) {
//        packet.dumpBytes();
        final long packetSeqNo = packet.getSeqNo();

        if ( receiver == null )
            return;

        if ( tmpPacket == null ) {
            tmpPacket = packetAllocator.newPointer(DataPacket.class);
        }

        packet.dataPointer(tmpStruct);
        final Bytez dataPacketBase = tmpStruct.getBase();
        final int dataindex = (int) tmpStruct.getOffset();
        final int packIndex = (int) packet.getOffset();

        decodeMsgBytes(packetSeqNo, dataPacketBase, dataindex, packIndex);
    }

    private void decodeMsgBytes(long packetSeqNo, Bytez dataPacketBase, int dataindex, int packIndex) {
        FCReceiveContext.get().setSender(receivesFrom);

        long now = System.currentTimeMillis();
        packCount++;
        if (now - lastPacket > 1000) {
            int persec = (int) ((packCount * 1000l) / (now - lastPacket));
            lastPacket = now;
            packCount = 0;
        }
        if (!isUnordered() && !isUnreliable() && debugPrevSeq != 0 && debugPrevSeq != packetSeqNo - 1) {
            FCLog.get().fatal("FATAL ERROR " + packetSeqNo);
            System.exit(1);
        }
        debugPrevSeq = packetSeqNo;

        currentPacketBytePointer.baseOn(dataPacketBase, dataindex);
        while (true) {
            short code = currentPacketBytePointer.getShort();
            if (code > DataPacket.MAX_CODE || code < 0) {
                FCLog.get().warn("foreign traffic or error assume delivered: " + maxDeliveredSeq.get() + " maxOrdered " + maxOrderedSeq.get() + " packseq " + packetSeqNo + " highest " + highestSeq);
                System.exit(1);
            }
            currentPacketBytePointer.next(2);
            if (code == DataPacket.EOP) {
                if (isUnordered()||isUnreliable()) {
                    tmpPacket.baseOn(dataPacketBase, packIndex);
                    tmpPacket.setDecoded(true);
                }
                maxDeliveredSeq.set(Math.max(maxDeliveredSeq.get(), packetSeqNo));
                return;
            } else {
                short len = currentPacketBytePointer.getShort();
                currentPacketBytePointer.next(2);
                if (inInitialSync) {
                    if (code == DataPacket.COMPLETE) {
                        inInitialSync = false;
                    }
                } else {
                    if ( (packetSeqNo&2047) == 0 ) {
                        if ( topicEntry.hadHeartbeat(receivesFrom) /* do not disturb bootstrap sequence */) {
                            // under high pressure heartbeats are squeezed, treat any 2048'th packet as heartbeat then
                            topicEntry.registerHeartBeat(receivesFrom, System.currentTimeMillis());
                        }
                    }
                    decoder.receiveChunk(packetSeqNo, currentPacketBytePointer.getBase(), (int) currentPacketBytePointer.getOffset(), len, code == DataPacket.COMPLETE);
                    stats.msgReceived();
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
        freeImmediate();
        terminated = true;
    }

    private void freeImmediate() {
        long alloced = MallocBytezAllocator.alloced.get();
        packetAllocator.free();
        long curr = MallocBytezAllocator.alloced.get();
        FCLog.log("freed " + (alloced - curr) / 1024 / 1024 + "MB to " + curr / 1024 / 1024 + " MB");
    }
}
