package de.ruedigermoeller.fastcast.packeting;

import de.ruedigermoeller.fastcast.util.FCLog;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/19/13
 * Time: 6:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class OffHeapHistory {

    static long CHUNK_SIZE = 10L * 1000L * 1000L;

    ByteBuffer buff[];

    int entrySize;
    int numEntries;
    int numEntriesPerBuff;

    public static OffHeapHistory createDirectHistory(long sizeBytes, int dgramSize) {
        OffHeapHistory res = new OffHeapHistory();
        res.init(dgramSize,sizeBytes);
        return res;
    }

    void init( int dgramSiz, long sizeBytes ) {
        entrySize = dgramSiz+12;
        while( entrySize % 8 != 0)
            entrySize++;
        numEntriesPerBuff = (int) (CHUNK_SIZE/entrySize);
        int numChunks = (int) (sizeBytes/((long)numEntriesPerBuff*dgramSiz))+1;
        numEntries = numChunks*numEntriesPerBuff;
        buff = new ByteBuffer[numChunks];
        FCLog.log("allocating "+(numChunks*CHUNK_SIZE/1000/1000)+"MByte of directmemory");
        for (int i = 0; i < buff.length; i++) {
            buff[i] = ByteBuffer.allocateDirect((int) CHUNK_SIZE);
        }
    }

    Thread previous = null;
    public void putPacket(long sequence, byte[] packet, int packOff, int packLen) {
        checkThreads();
        int bufNo = (int) (sequence/numEntriesPerBuff) % buff.length;
        int bufIdx = (int) (sequence%numEntriesPerBuff);
        ByteBuffer buffer = buff[bufNo];
        buffer.position((bufIdx % numEntries) * entrySize);
        buffer.putLong(sequence);
        buffer.putInt(packLen);
        buffer.put(packet, packOff, packLen);
    }

    private void checkThreads() {
//        if ( previous == null )
//            previous = Thread.currentThread();
//        if ( previous != Thread.currentThread() )
//            throw new RuntimeException("multiple threads in single threaded code");
    }

    public int getPacket(long sequence, byte[] dest, int off) {
        checkThreads();
        int bufNo = (int) (sequence/numEntriesPerBuff) % buff.length;
        int bufIdx = (int) (sequence%numEntriesPerBuff);
        ByteBuffer buffer = buff[bufNo];
        buffer.position((bufIdx% numEntries)*entrySize);
        long readSeq = buffer.getLong();
        if ( sequence == readSeq ) {
            int len = buffer.getInt();
            buffer.get(dest,off,len);
            return len;
        }
        return -1;
    }

    public boolean hasSequence(long sequence) {
        checkThreads();
        if ( previous == null )
            previous = Thread.currentThread();
        int bufNo = (int) (sequence/numEntriesPerBuff) % buff.length;
        int bufIdx = (int) (sequence%numEntriesPerBuff);
        ByteBuffer buffer = buff[bufNo];
        buffer.position((int) (bufIdx% numEntries)*entrySize);
        long readSeq = buffer.getLong();
        return sequence == readSeq;
    }


}
