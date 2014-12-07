package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.transport.PhysicalTransport;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.offheap.bytez.ByteSource;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.malloc.MallocBytez;
import org.nustaq.offheap.bytez.malloc.MallocBytezAllocator;
import org.nustaq.offheap.bytez.onheap.HeapBytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructArray;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/10/13
 * Time: 11:42 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * packet buffer backed by binary struct array. Can be used as sender with ring buffered history.
 */
public class PacketSendBuffer implements FCPublisher {

    public static final boolean RETRANSDEBUG = true;
    private static final int RETRANS_MEM = 10000; // retransrequest history to accumulate identical retrans requests
    private static final int TAG_BUFF = 4;
    private static final boolean DEBUG_LAT = false;
    final PhysicalTransport trans;

    FSTStructAllocator offheapAllocator;
    FSTStructAllocator heapAllocator;
    ConcurrentLinkedQueue<RetransPacket> retransRequests = new ConcurrentLinkedQueue<RetransPacket>();


    String nodeId;
    int payMaxLen;
    int currentAvail; // number of bytes avaiable in current package
    int topic;

    FSTStruct currentPacketBytePointer; // points to currently written packet

    // as long nextSendMsg == currentSequence, packet is in.write and cannot be sent
    long currentSequence = 1; // putmsg sequence
    long nextSendMsg = 1;     // first sequence avaiable to send (must be < currentSequence)

    // send history ringbuffer
    StructArray<DataPacket> history;
    int historySize;

    ControlPacket dropMsg; // prepared message for drop
    DataPacket template;   // template for new data packet

    ByteBuffer tmpSend;

    Topic topicEntry;
    boolean isUnordered;
    int packetRateLimit;
    int pps;
    int ppsWindowNanos;

    int packetCounter;
    long lastPpsRateCheckNanos;
    String currentReceiver = null;
    Bytez heartbeat;

    public PacketSendBuffer(PhysicalTransport trans, String nodeId, Topic entry ) {
        this.trans = trans;
        this.topic = entry.getTopicId();
        topicEntry = entry;
        this.nodeId = nodeId;
        this.hbInvtervalMS = entry.getPublisherConf().getHeartbeatInterval();

        FCLog.log( "init send buffer for topic "+entry.getTopicId() );

        final PhysicalTransportConf conf = trans.getConf();
        template = DataPacket.getTemplate(conf.getDgramsize());
        payMaxLen = template.data.length;

        template.getSender().setString(nodeId);
        template.setTopic(topic);

        offheapAllocator = new FSTStructAllocator(0, new MallocBytezAllocator());
        heapAllocator = new FSTStructAllocator(0);

        final PublisherConf publisherConf = topicEntry.getPublisherConf();
        int hSize = publisherConf.getNumPacketHistory();
        if ( ((long)hSize*conf.getDgramsize()) > Integer.MAX_VALUE-2*conf.getDgramsize() ) {
            final int newHist = (Integer.MAX_VALUE - 2 * conf.getDgramsize()) / conf.getDgramsize();
            publisherConf.setNumPacketHistory(newHist);
            FCLog.get().warn("int overflow, degrading history size from "+hSize+" to "+newHist);
            hSize = newHist;
        }
        history = offheapAllocator.newArray(hSize, template);
        FCLog.log("allocating send buffer for topic " + topicEntry.getTopicId() + " of " + history.getByteSize() / 1024 / 1024 + " MByte");
        historySize = history.size();

        setUnordered(topicEntry.isUnordered());

        initDropMsgPacket(nodeId);

        DataPacket curP = getPacketAt(currentSequence);
        currentPacketBytePointer = curP.detach();
        curP.setSeqNo(currentSequence);
        curP.dataPointer(currentPacketBytePointer);
        currentAvail = payMaxLen-TAG_BUFF; // room for tags
        try {
            initTmpBBuf();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        pps = publisherConf.getPps()/publisherConf.getPpsWindow();
        ppsWindowNanos = (1000*1000*1000)/publisherConf.getPpsWindow();
        heartbeat = new HeapBytez(new byte[]{ControlPacket.HEARTBEAT});
        flush(); // enforce immediate heartbeat
    }

    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));
        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    private void initTmpBBuf() throws NoSuchFieldException, IllegalAccessException {
        // patch MallocBytes such that it points to a DirectByteBuffer (allows zerocopy using fst bytez utils)
        tmpSend = ByteBuffer.allocateDirect(0);
        Field address = null;
        Field capacity = null;

        List<Field> fields = new ArrayList<>();
        getAllFields(fields, tmpSend.getClass());
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if ( field.getName().equals("address") ) {
                address = field;
            } else if ( field.getName().equals("capacity") ) {
                capacity = field;
            }
        }
        address.setAccessible(true);
        capacity.setAccessible(true);

        MallocBytez base = (MallocBytez) history.getBase();
        address.setLong(tmpSend, base.getBaseAdress() + history.getOffset());
        capacity.setInt(tmpSend, history.getByteSize());
    }

    private void moveBuff(DataPacket packet) {
        tmpSend.limit((int) (packet.getOffset() + packet.getDGramSize()));
        tmpSend.position((int) packet.getOffset());
    }

    protected void initDropMsgPacket(String nodeId) {
        dropMsg = new ControlPacket();
        dropMsg.getSender().setString(nodeId);
        dropMsg.setTopic(topic);
        dropMsg.setType(ControlPacket.DROPPED);
        dropMsg = heapAllocator.newStruct(dropMsg);
    }

    public void free() {
        offheapAllocator.free();
    }

    public Topic getTopicEntry() {
        return topicEntry;
    }

    public boolean isUnordered() {
        return isUnordered;
    }

    public void setUnordered(boolean unordered) {
        isUnordered = unordered;
    }

    private DataPacket getPacketAt(long seq) {
        return history.get(getIndexFromSequence(seq));
    }

    private int getIndexFromSequence(long seq) {
        return (int) (seq%historySize);
    }

    private boolean putMessage(int tag, ByteSource b, long offset, int len, boolean tryPut) {
        putMessageRecursive(tag, b, offset, len);
        return true;
    }

    private void putMessageRecursive(int tag, ByteSource b, long offset, int len) {
        while(true) {
            // payheader is type 2, len 2
            if ( currentAvail > len+DataPacket.HEADERLEN+2 )  // 2 byte needed for eop
            {
                // message fits into avaiable paylen
                putInternal(tag, DataPacket.COMPLETE, b, offset, len);
                return;
            } else {
                if ( isUnordered() ) {
                    if ( len > payMaxLen-DataPacket.HEADERLEN-2 ) {
                        throw new RuntimeException("unordered message size must not exceed packet size");
                    }
                    fire();
                    putMessageRecursive(tag, b,offset,len);
                    return;
                }
                // message must be chained, new packet required
                int sendlen = currentAvail - DataPacket.HEADERLEN - 8; //
                if ( sendlen <= 8 ) { // don't chain if only few bytes left
                    fire();
//                    if ( rec != null )
//                        getPacketAt(currentSequence.get()).getReceiver().setString(rec);
//                    putMessageRecursive(tag, b, offset, len, rec); stackoverflow
//                    return;
                } else
                {
                    // put chunk
                    putInternal(tag, DataPacket.CHAINED, b, offset, sendlen);
                    fire();
                    tag = -1; offset = offset+sendlen; len = len - sendlen;
                }
            }
        }
    }

    private void putInternal(int tag, short code, ByteSource b, long offset, int len) {
        int off = 0;
        if ( tag >= 0 ) {
            off = 1;
        }
        currentPacketBytePointer.setShort(code);
        currentPacketBytePointer.next(2);
        currentPacketBytePointer.setShort((short) (len + off));
        currentPacketBytePointer.next(2);
        if ( tag>=0 ) {
            currentPacketBytePointer.setByte((byte) tag);
            currentPacketBytePointer.next(off);
        }
        currentPacketBytePointer.setBytes(b, offset, len); // fixme memcpy optimization missing
        currentPacketBytePointer.next(len);
        currentAvail= currentAvail-(len+off+DataPacket.HEADERLEN);
    }


    // finishes current packet, and allocs a new one so packet can be sent
    private void fire() {
        if (DEBUG_LAT)
            System.out.println("fire "+System.currentTimeMillis());
        if (isCurrentPacketEmpty()) // no message yet in packet
            return;
        currentPacketBytePointer.setShort(DataPacket.EOP);
        long curSeq = currentSequence;
        currentAvail-=2; // for EOP
        if ( currentAvail < 0 )
            throw new RuntimeException("negative bytes left "+currentAvail);
        getPacketAt(curSeq).setBytesLeft(currentAvail);

        long newSeq = curSeq + 1;
        DataPacket newPack = getPacketAt(newSeq);
        newPack.dataPointer(currentPacketBytePointer);
        newPack.setSeqNo(newSeq);
        currentAvail = payMaxLen-TAG_BUFF; // safe to always put a tag

        if ( currentReceiver != null )
            newPack.getReceiver().setString(currentReceiver);

        currentSequence++; // publish packet
    }

    /**
     * @return true if current packet does not contain any payload
     */
    private boolean isCurrentPacketEmpty() {
        return currentAvail == payMaxLen - TAG_BUFF;
    }

    /**
     * send pending packets to transport. Concurrent adding threads are assumed to not overtake.
     *
     * @throws IOException
     */
    private boolean sendPendingPackets() throws IOException {
        if ( currentSequence <= nextSendMsg ) {
            return false;
        }
        sendPackets(nextSendMsg, currentSequence, false, 0);
        return true;
    }

    long sentRetransSeq[] = new long[RETRANS_MEM];
    long sentRetransTimes[] = new long[RETRANS_MEM];
    private boolean sendPendingRetrans() throws IOException {
        // send retransmission
        if ( retransRequests.peek() != null ) {
            ArrayList<RetransPacket> curRetrans = new ArrayList<>();
            RetransPacket poll = null;
            do {
                poll = retransRequests.poll();
                curRetrans.add(poll);
            } while ( poll != null );
            mergeRetransmissions(curRetrans);
            return true;
        }
        return false;
    }

    private void putRetransSent(long sequence, long tim ) {
        int index = (int) (sequence % RETRANS_MEM);
        sentRetransSeq[index] = sequence;
        sentRetransTimes[index] = tim;
    }

    private long getLastRetransmitted(long sequence) {
        int index = (int) (sequence % RETRANS_MEM);
        if ( sentRetransSeq[index] == sequence )
            return sentRetransTimes[index];
        return 0;
    }

    private void mergeRetransmissions(ArrayList<RetransPacket> curRetrans) throws IOException {
        long now = System.currentTimeMillis();
        for (int i = 0; i < curRetrans.size(); i++) {
            RetransPacket retransPacket = curRetrans.get(i);
            if ( retransPacket != null )
                sendRetransmissionResponse(retransPacket, now);
        }
    }

    int maxRetransAge = 0;
    private void sendRetransmissionResponse(RetransPacket retransPacket, long now) throws IOException {
        if ( RETRANSDEBUG )
            FCLog.get().net("enter retrans " + retransPacket);
        for ( int ii = 0; ii < retransPacket.getRetransIndex(); ii++ ) {
            RetransEntry en = retransPacket.retransEntries(ii);
            if ( RETRANSDEBUG ) {
                FCLog.get().net( System.currentTimeMillis()+" retransmitting " + en);
            }
            long fromSeqNo = getPacketAt(en.getFrom()).getSeqNo();
            // note 'from' is oldest, so if from exists, all following exist also !
            if (fromSeqNo != en.getFrom() ) // not in on heap history ?
            {
                fromSeqNo = en.getFrom();
                if ( currentSequence-fromSeqNo > maxRetransAge ) {
                    maxRetransAge = (int) (currentSequence-fromSeqNo);
                    FCLog.get().warn("old retransmission from " + retransPacket.getSender() + " age " + maxRetransAge + " requested:" + fromSeqNo + " curseq " + currentSequence + " topic " + topicEntry.getTopicId());
                }
                dropMsg.setReceiver(retransPacket.getSender());
                dropMsg.setSeqNo(en.getFrom());
                FCLog.get().warn("Sending Drop " + dropMsg + " requestedSeq " + fromSeqNo + " on service " + getTopicEntry().getTopicId() + " currentSeq " + currentSequence + " age: " + (currentSequence - en.getFrom()));
                packetCounter++;
                trans.send(new DatagramPacket(dropMsg.getBase().toBytes((int) dropMsg.getOffset(), dropMsg.getByteSize()), 0,dropMsg.getByteSize()));
            } else {
                long from = en.getFrom();
                long to = en.getTo();
                sendPackets(from, to, true, now);
            }
        }
    }

    int suppressedRetransCount = 0;
    ThreadLocal<byte[]> msgBytes = new ThreadLocal<>();
    private void sendPackets(long sendStart, long sendEnd, boolean retrans, long now /*only set for retrans !*/) throws IOException {
        for ( long i = sendStart; i < sendEnd; i++ ) {
            DataPacket dataPacket = getPacketAt(i);
            if ( retrans ) {
//                if ( now - getLastRetransmitted(i) < MaxRetransRepeatIntervalMS ) {
//                    suppressedRetransCount++;
//                    continue;
//                } else
                {
                    putRetransSent(i,now);
                }
            } else {
                if ( dataPacket.getSeqNo() != i )
                {
                    FCLog.get().fatal("FATAL error, current seq:"+currentSequence+" expected Seq:["+i+"] real read:"+dataPacket.getSeqNo());
                    FCLog.get().fatal("current put seq "+currentSequence);
                    FCLog.get().fatal("current send seq "+nextSendMsg);
                    FCLog.get().fatal("current pointer and currentpackpointer "+dataPacket.___offset+" "+currentPacketBytePointer.___offset);
                    FCLog.get().fatal(null,new Exception("stack trace"));
                    for (int ii = 0; ii < 20; ii++)
                        FCLog.get().fatal("  =>"+ getPacketAt(i + ii).getSeqNo());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
                    }
                    System.exit(1);
                }
            }
            moveBuff(dataPacket);
            packetCounter++;
            trans.send(tmpSend);
        }
        if ( ! retrans ) {
            nextSendMsg = sendEnd;
        }
    }

    void addRetransmissionRequest(RetransPacket retransPacket, PhysicalTransport trans) throws IOException {
        RetransPacket copy = (RetransPacket) retransPacket.createCopy();
        if ( RETRANSDEBUG )
            System.out.println("received retrans request and add to Q " + copy);
        retransRequests.add(copy);
        flush();
    }

    AtomicBoolean sendLock = new AtomicBoolean(false);
    private void lock() {
        while( ! sendLock.compareAndSet(false,true) ) {
        }
    }

    private void unlock() {
        sendLock.set(false);
    }
    long hbInvtervalMS = 1000; // FIXME: use settings
    long lastHB = 0;

    public volatile long lastFlush = System.currentTimeMillis();

    protected boolean offerNoLock(ByteSource msg, long start, int len, boolean doFlush) {
        long now = System.nanoTime();
        if ( now-lastPpsRateCheckNanos > ppsWindowNanos ) {
            packetCounter = Math.max(0,packetCounter-pps);
            lastPpsRateCheckNanos = now;
        }
        if (msg != null ) {
            if ( packetCounter > pps*2 )
                return false;
            else if (packetCounter > pps) {
                doFlush = false;
            }
        }
        boolean res;
        if ( msg == null ) {
            res = true;
        } else {
            res = putMessage(-1,msg,start,len, true);
        }
        if ( now-lastHB > hbInvtervalMS*1000*1000 ) {
            lastHB = now;
            setReceiver(null);
            return offerNoLock(heartbeat,0,1,true);
        }
        if ( doFlush ) {
            lastFlush = now;
            fire();
        }
        try {
            if ( ! sendPendingRetrans() ) {
                if ( sendPendingPackets() ) {
                    lastFlush = now;
                }
            } else
                res = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // publisher interface
    //
    @Override
    public boolean offer(ByteSource msg, long start, int len, boolean doFlush) {
        try {
            lock();
//            synchronized (this)
            {
                return offerNoLock(msg, start, len, doFlush);
            }
        } finally {
            unlock();
        }
    }

    @Override
    public boolean offer(ByteSource msg, boolean doFlush) {
        try {
            lock();
//            synchronized (this)
            {
                return offerNoLock(msg, 0, (int) msg.length(), doFlush);
            }
        } finally {
            unlock();
        }
    }

    @Override
    public int getTopicId() {
        return topicEntry.getTopicId();
    }

    @Override
    public void setReceiver(String receiverNodeId) {
        if ( receiverNodeId == null ) {
            if ( currentReceiver != null ) {
                currentReceiver = null;
                updateCurrentReceiver();
            } else {
                return; // nothing changed
            }
        } else if ( receiverNodeId.equals(currentReceiver) ) {
            return;
        } else {
            currentReceiver = receiverNodeId;
            updateCurrentReceiver();
        }
    }

    private void updateCurrentReceiver() {
        if ( isCurrentPacketEmpty() ) {
            getPacketAt(currentSequence).getReceiver().setString(currentReceiver);
        } else {
            fire();
            updateCurrentReceiver();
        }
    }

    @Override
    public void setPacketRateLimit(int limit) {
        packetRateLimit = limit;
    }

    @Override
    public int getPacketRateLimit() {
        return packetRateLimit;
    }

    @Override
    public void flush() {
        offer(null,0,0,true);
    }

}
