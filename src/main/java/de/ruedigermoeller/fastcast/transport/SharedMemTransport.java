package de.ruedigermoeller.fastcast.transport;

import de.ruedigermoeller.fastcast.util.FCLog;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
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
 * Date: 25.05.13
 * Time: 19:19
 * To change this template use File | Settings | File Templates.
 */

/**
 * a shared memory transport implementation using public API (no unsafe or dirty hacks). It uses memory mapped
 * files and filechannel.lock to synchronize concurrent access.
 * Latency (datagramsize lesser 1000, lowlat config) is 6-7 microseconds on i7/win8, ~12 micros on AMD opteron 2.2Ghz/CentOS.
 * Throughput is 30% higher compared to local host socket (as long decoding is not the limiting factor). Latency has less jitter
 * and is ~ twice as low compared to local host multicast socket.
 */
public class SharedMemTransport implements Transport {

    final static int QENTRY_HEADER = 8;
    MappedByteBuffer mWriteQueue;
    RandomAccessFile mRandomFile;
    FileChannel mChannel;
    int dgramSize;
    int packSiz;

    long address;
    int numPack;

    long currentReadSequence;
    private long currentWriteSequence;
    FCSocketConf conf;
    ByteBuffer mReadQueue;


    public SharedMemTransport(FCSocketConf c) {
        this(c.getQueueFile(),c.getDgramsize(),c.getReceiveBufferSize());
        conf = c;
    }

    public SharedMemTransport(String path, int datagramsiz, int buffsiz) {
        this.dgramSize = datagramsiz;
        packSiz = dgramSize+QENTRY_HEADER;
        numPack = buffsiz/packSiz;
        try {
            initMemMap(path,packSiz*numPack+packSiz);
        } catch (Exception e) {
            FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public long getPack(long index) {
        return address+(index%numPack)* packSiz;
    }

    public int getPackOff(long index) {
        return (int) ((index%numPack)* packSiz);
    }

    byte[] zeros;
    public synchronized void write(byte[] b, int off, int len) {
        FileLock lock = null;
        try {
            lock = mChannel.lock();
            long seq = 0;
            int pos = getPackOff(currentWriteSequence);
            while ( (seq= mWriteQueue.getLong(pos)) >= currentWriteSequence ) {
                if ( seq != 0 )
                    currentWriteSequence = seq+1;
                else
                    currentWriteSequence++;
                pos = getPackOff(currentWriteSequence);
            }
            mWriteQueue.position(pos);
            mWriteQueue.putLong(currentWriteSequence);
            mWriteQueue.put(b, off, len);
            mWriteQueue.put(zeros, 0, dgramSize - len);
        } catch (IOException e) {
            FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
//        System.out.println("wrote " + currentWriteSequence + " " + System.nanoTime());
        currentWriteSequence++;
    }

    long findMaxSequence() {
        FileLock lock = null;
        long res = 1;
        try {
            lock = mChannel.lock();
            positionQueue(mWriteQueue,0);
            int pos = mWriteQueue.position();
            for (int i=0; i < numPack-1; i++) {
                res = Math.max( mWriteQueue.getLong(pos), res);
                pos += packSiz;
            }
        } catch (IOException e) {
            FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
        return res;
    }

    public boolean tryRead( byte[] b, int off ) {
        FileLock lock = null;

        positionQueue(mReadQueue,currentReadSequence);
        long seq = mReadQueue.getLong();
        if ( seq >= currentReadSequence ) {
            synchronized ( this ) {
                try {
                    lock = mChannel.lock();
                    positionQueue(mReadQueue,currentReadSequence);
                    seq = mReadQueue.getLong();
                    try {
                        mReadQueue.get(b, off, dgramSize);
                    } catch (Exception e) {
                        throw new RuntimeException("index "+ mReadQueue.position()+" readSiz "+dgramSize, e);
                    }
                    lock.release(); lock = null;
                    currentReadSequence = seq+1;
    //                System.out.println("got packet " + (currentReadSequence-1) + " " + System.nanoTime()+" "+b[0]+","+b[1]+","+b[3]);
                    return true;
                } catch (IOException e) {
                    FCLog.get().fatal("Thread " + Thread.currentThread().getName(),e);
                } finally {
                    if (lock != null) {
                        try {
                            lock.release();
                        } catch (IOException e) {
                            FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }
            }
        }
//            Thread.yield();
        return false;
    }

    private void positionQueue(ByteBuffer buff, long seq) {
        buff.position((int) (packSiz * (seq % numPack)));
    }

    protected void initMemMap( String filePath, int siz ) throws IOException, NoSuchFieldException, IllegalAccessException {
        mRandomFile = new RandomAccessFile(filePath, "rw");
        mRandomFile.setLength(siz);
        mChannel = mRandomFile.getChannel();
        mWriteQueue = mChannel.map(FileChannel.MapMode.READ_WRITE, 0, siz);
        mWriteQueue.load();
        mReadQueue = mWriteQueue.duplicate();
        Field f = Buffer.class.getDeclaredField("address");
        f.setAccessible(true);
        address = (Long)f.get(mWriteQueue);
        zeros = new byte[dgramSize];
//        System.out.println("got address "+address);

//        long tim = System.currentTimeMillis();
//        for ( int n = 0; n < 1000000; n++ ) {
//            FileLock lock = mChannel.lock();
//            unsafe.putLong(address+11000, (long) (Math.random()*1000));
//            lock.release();
//        }
//        System.out.println("lock time "+(System.currentTimeMillis()-tim));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // impl of transport interfcae
    //

    @Override
    public boolean receive(DatagramPacket pack) throws IOException {
        return tryRead(pack.getData(),pack.getOffset());
    }

    @Override
    public void send(DatagramPacket pack) throws IOException {
        write(pack.getData(),pack.getOffset(),pack.getLength());
    }

    volatile boolean joined = false;
    @Override
    public synchronized void join() throws IOException {
        if ( joined )
            return;
        joined = true;
        currentWriteSequence = findMaxSequence()+1;
        currentReadSequence = currentWriteSequence;
//        System.out.println("listen at seq "+currentReadSequence+", write to "+currentWriteSequence);
    }

    @Override
    public FCSocketConf getConf() {
        return conf;
    }

    public static void main( String arg[] ) throws Exception {
        SharedMemTransport shmem = new SharedMemTransport("\\test\\queue.txt",8000,400000000);
        shmem.join();
        if ( arg.length > 0 ) {
            byte read[] = new byte[9000];
            while( true )
                shmem.tryRead(read,0);
        } else {
            byte write[] = {1,3,5,7,9,2,4,6,8,10};
            while( true ) {
                Thread.sleep(1);
                shmem.write(write,0,write.length);
            }
        }
    }


}
