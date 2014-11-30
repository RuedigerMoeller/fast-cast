package org.nustaq.fastcast.packeting;

import org.nustaq.fastcast.control.FlowControl;
import org.nustaq.fastcast.remoting.FCPublisher;
import org.nustaq.fastcast.transport.Transport;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.ByteSource;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.malloc.MallocBytezAllocator;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructArray;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    public static final int MAX_BULK_SEND_DATA = 2;
    public static final boolean RETRANSDEBUG = false;
    private static final int RETRANS_MEM = 10000;
    private static final int TAG_BUFF = 4;
    public static final int RETRANS_PACKET_PAUSE_THRESHOLD = 5; // if retrans < this, skip sendPause
    private static final boolean DEBUG_LAT = false;
    FSTStructAllocator packetAllocator;
    ArrayList<RetransPacket> retransRequests = new ArrayList<RetransPacket>();

    StructArray<DataPacket> history;
    String nodeId;
    int payMaxLen;
    volatile int currentAvail; // number of bytes avaiable in current package
    int topic;

    AtomicLong currentSequence = new AtomicLong(1); // putmsg sequence
    AtomicLong nextSendMsg = new AtomicLong(1);     // sendmsg sequence
    FSTStruct currentPacketBytePointer;
    ControlPacket dropMsg;

    Sleeper sleeperSendMsg = new Sleeper();
    int sendPauseMicros = 100;

    private Lock putLock = new ReentrantLock();
    int maxSendPacketsQueued = 4 * MAX_BULK_SEND_DATA;
    boolean isUnordered;
    boolean optForLatency = false; // do not chain messages across datagrams except msg > datagram size
    TopicEntry topicEntry;
    boolean useSpinLock;
    private Object sendWakeupLock = new Object[1];

    TopicStats stats;
    FlowControl control;
    long MaxRetransRepeatIntervalMS = 2;
    int historySize;
    int maxDGramRate;

    RateMeasure rate = new RateMeasure("sendDgramRate",100) {
        @Override
        protected void statsUpdated(long lastRatePersecond) {
//            super.statsUpdated(lastRatePersecond);
            if (maxDGramRate>0 && hasSendPressure()) {
                float percent = (float)lastRatePersecond/maxDGramRate;
//                System.out.println("  perc "+percent+" cur slow "+sendPauseMicros);
                sendPauseMicros = (int) ((sendPauseMicros*percent+1*sendPauseMicros)/2);
                if (sendPauseMicros<1)
                    sendPauseMicros = 1;
                if (sendPauseMicros < 10 && percent>1.2)
                    sendPauseMicros++;
//                System.out.println("  new slowdown "+sendPauseMicros+" queue "+(currentSequence.get()-nextSendMsg.get()));
            }
        }
    };

    DataPacket template;
    public PacketSendBuffer(int datagramSizeBytes, String clusterName, String nodeId, TopicEntry entry ) {
        this.topic = entry.getTopicId();
        topicEntry = entry;
        this.nodeId = nodeId;

        setOptForLatency(entry.getPublisherConf().isOptForLatency());
        useSpinLock = false; //entry.getConf().useSpinlockInSendQueue();

        FCLog.log( "init send buffer for topic "+entry.getTopicId() );

        template = DataPacket.getTemplate(datagramSizeBytes);
        payMaxLen = template.data.length;

        template.getCluster().setString(clusterName);
        template.getSender().setString(nodeId);
        template.setTopic(topic);

        packetAllocator = new FSTStructAllocator(10, new MallocBytezAllocator());
//        packetAllocator = new FSTTypedStructAllocator<DataPacket>(template,10);

        history = packetAllocator.newArray(topicEntry.getPublisherConf().getNumPacketHistory(), template);
        FCLog.log("allocating send buffer for topic " + topicEntry.getTopicId() + " of " + history.getByteSize() / 1024 / 1024 + " MByte");
        historySize = history.size();

        dropMsg = new ControlPacket();
        dropMsg.getCluster().setString(clusterName);
        dropMsg.getSender().setString(nodeId);
        dropMsg.setTopic(topic);
        dropMsg.setType(ControlPacket.DROPPED);
        dropMsg = packetAllocator.newStruct(dropMsg);

        DataPacket curP = getVolatile(currentSequence.get());
        currentPacketBytePointer = curP.detach();
        curP.setSeqNo(currentSequence.get());
        curP.dataPointer(currentPacketBytePointer);
        currentAvail = payMaxLen-TAG_BUFF; // room for tags
        setMaxSendPacketsQueued(0);
        setUnordered(topicEntry.isUnordered());
        this.sendPauseMicros = topicEntry.getPublisherConf().getSendPauseMicros();
        this.stats = topicEntry.getStats();
        this.control = topicEntry.getControl();
        maxDGramRate = topicEntry.getPublisherConf().getDGramRate();
    }

    public void free() {
        packetAllocator.free();
    }

    public TopicEntry getTopicEntry() {
        return topicEntry;
    }

    public TopicStats getStats() {
        return stats;
    }


    public boolean isUnordered() {
        return isUnordered;
    }

    public void setUnordered(boolean unordered) {
        isUnordered = unordered;
    }

    public int getMaxSendPacketsQueued() {
        return maxSendPacketsQueued;
    }

    public void setMaxSendPacketsQueued(int maxSendPacketsQueued) {
//        if (maxSendPacketsQueued == 0)
        {
            this.maxSendPacketsQueued = 0;
        }
//        else {
//            this.maxSendPacketsQueued = Math.max(maxSendPacketsQueued,4*MAX_BULK_SEND_DATA);
//        }
    }

    DataPacket getVolatile(long seq) {
        return history.get(getIndexFromSequence(seq));
    }

    private int getIndexFromSequence(long seq) {
        return (int) (seq%historySize);
    }

    public boolean putMessage(int tag, ByteSource b, int offset, int len, boolean tryPut) {
        if (DEBUG_LAT)
            System.out.println("pm "+System.currentTimeMillis());
        if ( maxSendPacketsQueued == 0 ) // no sender thread
            return putMessageST(tag, b, offset, len, tryPut);
        else
            return putMessageMT(tag, b, offset, len, tryPut);
    }

    public boolean putMessageST(int tag, ByteSource b, int offset, int len, boolean tryPut) {
//        if ( rec != null )
//            getVolatile(currentSequence.get()).getReceiver().setString(rec);
        putMessageRecursive(tag, b, offset, len);
        fire();
        try {
            sendPackets(topicEntry.getTrans(),nextSendMsg.get(),currentSequence.get(),false,0);
        } catch (IOException e) {
            FCLog.log(e);
        }
        return true;
    }

    public boolean putMessageMT(int tag, ByteSource b, int offset, int len, boolean tryPut) {
        putLock.lock();
        try {
            if (waitForSenderMT(len, tryPut)) return false;

            putMessageRecursive(tag, b, offset, len);
        } finally {
            putLock.unlock();
        }
        return true;
    }

    boolean hasSendPressure() {
        return currentSequence.get()-nextSendMsg.get() > 2;
    }

    // returns true if no space avaiable, false if space is avaiable ..
    private boolean waitForSenderMT(int len, boolean tryPut) {
        if ( maxSendPacketsQueued == 0 ) { // single threaded
            return false;
        }
        long nextQSeqNo = currentSequence.get() + 1;
        long currentSendSeqNo = nextSendMsg.get();
        int packetsRequired = len/payMaxLen+1;

        // wait until sender caught up
        while ( nextQSeqNo - currentSendSeqNo > maxSendPacketsQueued+packetsRequired || retransRequests.size() > 0 )
        { // don't run ahead more than this, also block in case of pending retrans
            if ( tryPut ) // leave in this case
                return true;
            putLock.unlock(); // give send thread a shot
            Thread.yield();
            putLock.lock();
            nextQSeqNo = currentSequence.get()+1; // unneeded ?
            currentSendSeqNo = nextSendMsg.get();
        }
        return false;
    }

    private void putMessageRecursive(int tag, ByteSource b, int offset, int len) {
        int loopCount = 0;
        while(true) {
            stats.msgSent();
            // payheader is type 2, len 2
            if ( currentAvail > len+DataPacket.HEADERLEN+2 )  // 2 byte needed for eop
            {
                // message fits into avaiable paylen
                putInternal(tag, DataPacket.COMPLETE, b, offset, len);
                if ( nextSendMsg.get() == currentSequence.get() )
                {
                    fire();
                }
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
//                        getVolatile(currentSequence.get()).getReceiver().setString(rec);
//                    putMessageRecursive(tag, b, offset, len, rec); stackoverflow
//                    return;
                } else
                {
                    // put chunk
                    putInternal(tag, DataPacket.CHAINED, b, offset, sendlen);
                    fire();
                    tag = -1; offset = offset+sendlen; len = len - sendlen;
                    loopCount++;
                    if ( loopCount >= maxSendPacketsQueued ) { // need to avoid complet buffer to get filled with sendpackets
                        waitForSenderMT(len,false); // wait until space free in queue again
                        loopCount = 0;
                    }
                }
            }
        }
    }

    public boolean isOptForLatency() {
        return optForLatency;
    }

    public boolean useSpinLock() {
        return useSpinLock && maxSendPacketsQueued > 0;
    }

    public void setOptForLatency(boolean optForLatency) {
        this.optForLatency = optForLatency;
    }

    private void putInternal(int tag, short code, ByteSource b, int offset, int len) {
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
        if ( currentAvail == payMaxLen )
            return;
        currentPacketBytePointer.setShort(DataPacket.EOP);
        long curSeq = currentSequence.get();
        currentAvail-=2; // for EOP
        if ( currentAvail < 0 )
            throw new RuntimeException("negative bytes left "+currentAvail);
        getVolatile(curSeq).setBytesLeft(currentAvail);

        long newSeq = curSeq + 1;
        DataPacket newPack = getVolatile(newSeq);
        newPack.dataPointer(currentPacketBytePointer);
        newPack.setSeqNo(newSeq);
        currentAvail = payMaxLen-TAG_BUFF; // safe to always put a tag
        newPack.setSent(System.currentTimeMillis());
        currentSequence.incrementAndGet();
        if ( ! useSpinLock() ) {
            synchronized (sendWakeupLock) {
                sendWakeupLock.notify();
            }
        }
    }

    /**
     * send pending packets to transport. Concurrent adding threads are assumed to not overtake.
     *
     * @param transport
     * @throws IOException
     */
    public boolean send(Transport transport) throws IOException {
        boolean anythingSent = false;
        // send retransmission
        if ( retransRequests.size() > 0 ) {
            anythingSent = true;
            ArrayList<RetransPacket> curRetrans = retransRequests;
            retransRequests = new ArrayList<RetransPacket>();

            mergeRetransmissions(transport, curRetrans);
            // FIXME: reuse retrans requests
        } else {
//            System.out.println("no retrans");
        }

        long sendStart;
        long sendEnd;

        sendStart = nextSendMsg.get();
        sendEnd = Math.min(sendStart + MAX_BULK_SEND_DATA, currentSequence.get());
        if ( sendEnd == sendStart ) {
            return anythingSent;
        }
        sendPackets(transport, sendStart, sendEnd, false, 0);
        return true;
    }

    
    long sentRetransSeq[] = new long[RETRANS_MEM];
    long sentRetransTimes[] = new long[RETRANS_MEM];

    void putRetransSent(long sequence, long tim ) {
        int index = (int) (sequence % RETRANS_MEM);
        sentRetransSeq[index] = sequence;
        sentRetransTimes[index] = tim;
    }

    long getLastRetransmitted(long sequence) {
        int index = (int) (sequence % RETRANS_MEM);
        if ( sentRetransSeq[index] == sequence )
            return sentRetransTimes[index];
        return 0;
    }

    private void mergeRetransmissions(Transport transport, ArrayList<RetransPacket> curRetrans) throws IOException {
        long now = System.currentTimeMillis();
        for (int i = 0; i < curRetrans.size(); i++) {
            RetransPacket retransPacket = curRetrans.get(i);
            if ( retransPacket != null )
                sendRetransmissionResponse(transport, retransPacket, now);
        }
    }

    int maxRetransAge = 0;
    private void sendRetransmissionResponse(Transport transport, RetransPacket retransPacket, long now) throws IOException {
        if ( RETRANSDEBUG )
            FCLog.get().net("enter retrans " + retransPacket);
        for ( int ii = 0; ii < retransPacket.getRetransIndex(); ii++ ) {
            RetransEntry en = retransPacket.retransEntries(ii);
            if ( RETRANSDEBUG ) {
                FCLog.get().net( System.currentTimeMillis()+" retransmitting " + en);
            }
            putLock.lock();
            long fromSeqNo = getVolatile(en.getFrom()).getSeqNo();
            // note 'from' is oldest, so if from exists, all following exist also !
            if (fromSeqNo != en.getFrom() ) // not in on heap history ?
            {
                fromSeqNo = en.getFrom();
                if ( currentSequence.get()-fromSeqNo > maxRetransAge ) {
                    maxRetransAge = (int) (currentSequence.get()-fromSeqNo);
                    FCLog.get().warn("old retransmission from "+retransPacket.getSender()+" age "+maxRetransAge+" requested:"+fromSeqNo+" curseq "+currentSequence.get()+" topic "+topicEntry.getReceiverConf().getTopicId() );
                }
                dropMsg.setReceiver(retransPacket.getSender());
                dropMsg.setSeqNo(en.getFrom());
                FCLog.get().warn("Sending Drop " + dropMsg + " requestedSeq " + fromSeqNo+" on service "+getTopicEntry().getReceiverConf().getTopicId()+" currentSeq "+currentSequence+" age: "+(currentSequence.get()-en.getFrom() ) );
                transport.send(new DatagramPacket(dropMsg.getBase().toBytes((int) dropMsg.getOffset(), dropMsg.getByteSize()), 0,dropMsg.getByteSize()));
            } else {
                sendPackets(transport, en.getFrom(), en.getTo(), true, now);
            }
            putLock.unlock();
        }
        int retransPacketsSummed = retransPacket.computeNumPackets();
        for ( int i = 0; i < retransPacketsSummed/4; i++ ) { // FIXME: hardcode: slow down max 25% in case of retrans
            sleeperSendMsg.sleepMicros(sendPauseMicros);
        }
    }

    long debugSeq = 0; int suppressedRetransCount = 0;
    ThreadLocal<byte[]> msgBytes = new ThreadLocal<>();
    private void sendPackets(Transport transport, long sendStart, long sendEnd, boolean retrans, long now /*only set for retrans !*/) throws IOException {
//        boolean doPause = true; // !retrans || sendEnd-sendStart > RETRANS_PACKET_PAUSE_THRESHOLD;
        for ( long i = sendStart; i < sendEnd; i++ ) {
            DataPacket dataPacket = getVolatile(i);
            if ( retrans ) {
//                if ( now - getLastRetransmitted(i) < MaxRetransRepeatIntervalMS ) {
//                    suppressedRetransCount++;
//                    continue;
//                } else
                {
                    putRetransSent(i,now);
                }
            } else {
                if ( debugSeq != 0 && dataPacket.getSeqNo() != debugSeq+1 )
                {
                    FCLog.get().fatal("FATAL error, current seq:"+debugSeq+" expected Seq:["+i+"] real read:"+dataPacket.getSeqNo());
                    FCLog.get().fatal("current put seq "+currentSequence.get());
                    FCLog.get().fatal("current send seq "+nextSendMsg.get());
                    FCLog.get().fatal("current pointer and currentpackpointer "+dataPacket.___offset+" "+currentPacketBytePointer.___offset);
                    FCLog.get().fatal(null,new Exception("stack trace"));
                    for (int ii = 0; ii < 20; ii++)
                        FCLog.get().fatal("  =>"+getVolatile(i+ii).getSeqNo());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
                    }
                    System.exit(1);
                }
                debugSeq = dataPacket.getSeqNo();
//                if ( doPause )
                dataPacket.setSendPauseSender(sendPauseMicros);
            }

            sleeperSendMsg.sleepMicros(sendPauseMicros);

            int dGramSize = dataPacket.getDGramSize();
            try {
//              if ( dataPacket.getDGramSize() > 7998 ) {
//                  throw new RuntimeException("packet to large left "+dataPacket.getBytesLeft()+" bsize "+dataPacket.getByteSize()+" paymax "+payMaxLen);
//              }

                byte b[] = msgBytes.get();
                if (b==null)
                {
                    b = new byte[transport.getConf().getDgramsize()];
                    msgBytes.set(b);
                }
                dataPacket.getBytes(b,0,dGramSize);
                DatagramPacket pack = new DatagramPacket(b, 0, dGramSize);

//                DatagramPacket pack = new DatagramPacket(dataPacket.getBase().asByteArray(), (int) dataPacket.getOffset(), dGramSize);

                if (DEBUG_LAT)
                    System.out.println("send "+System.currentTimeMillis());
                rate.count();
                transport.send(pack);
                if ( retrans ) {
                    stats.retransRSPSent(1,dGramSize);
                } else {
                    stats.dataPacketSent(dGramSize);
                }
            } catch ( Throwable th) {
                System.out.println("seq "+i+" start "+sendStart+" end "+sendEnd+" idx "+getIndexFromSequence(i)+" len "+(dataPacket.getBase().length())+" off+siz "+(dataPacket.getOffset()+ dGramSize));
                throw new RuntimeException(th);
            }

        }
        if ( ! retrans ) {
            nextSendMsg.set(sendEnd);
        }
    }

    public void addRetransmissionRequest(RetransPacket retransPacket, Transport trans) throws IOException {
        RetransPacket copy = (RetransPacket) retransPacket.createCopy();
        stats.retransRQReceived(copy.computeNumPackets(),copy.getSendPauseSender());
        if ( RETRANSDEBUG )
            System.out.println("received retrans request and add to Q " + copy);
        retransRequests.add(copy);
    }

    public Object getSendWakeupLock() {
        return sendWakeupLock;
    }

    public void doFlowControl() {
        if ( control == null ) {
            // BUG, receiverService should not do flowcontrol !
//            control = topicEntry.getService();
        }
        int tmp = sendPauseMicros;
        if ( control != null ) {
            control.adjustSendPause(sendPauseMicros, stats);
            sendPauseMicros = Math.max(tmp, topicEntry.getPublisherConf().getSendPauseMicros());
            sendPauseMicros = Math.min(sendPauseMicros, 2 * topicEntry.getPublisherConf().getSendPauseMicros());
        }
        stats.setLastSendPause(sendPauseMicros);
        stats.reset();
    }

    @Override
    public boolean offer(ByteSource msg, int start, int len) {
        return putMessage(-1,msg,start,len, true);
    }

    @Override
    public int getTopicId() {
        return 0;
    }
}
