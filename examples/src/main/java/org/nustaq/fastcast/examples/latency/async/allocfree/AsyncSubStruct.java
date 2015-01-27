package org.nustaq.fastcast.examples.latency.async.allocfree;

import org.HdrHistogram.Histogram;
import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;
import org.nustaq.fastcast.examples.latency.async.AsyncLatMessage;
import org.nustaq.fastcast.examples.latency.async.AsyncLatPublisher;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by ruedi on 26/01/15.
 *
 * Note the implementation is not allocation free but has a significant reduced alloc rate.
 * A completely alloc free version would need a custom executor reading from a array based concurrent queue,
 * which is not part of JDK.
 */
public class AsyncSubStruct {
    FastCast fastCast;
    private FCPublisher backPub; // pong for rtt measurement

    StupidPool pool = new StupidPool(1024);
    Executor bounceBackExec = Executors.newSingleThreadExecutor();

    public void initFastCast() throws Exception {
        fastCast =  FastCast.getFastCast();
        fastCast.setNodeId("SUB");
        fastCast.loadConfig(AsyncLatPublisher.CFG_FILE_PATH);

        backPub = fastCast.onTransport("back").publish("back");

        // pointer to message
        final FSTStruct msg = FSTStructFactory.getInstance().createEmptyStructPointer(FSTStruct.class);

        final RateMeasure measure = new RateMeasure("receive rate");
        fastCast.onTransport("default").subscribe("stream",
            new FCSubscriber() {
                @Override
                public void messageReceived(String sender, long sequence, Bytez b, long off, final int len) {
                    measure.count();
                    final byte copy[] = pool.getBA();
                    if ( len < copy.length ) // prevent segfault :) !
                    {
                        b.getArr(off,copy,0,len);
                        // do bounce back in different thread, else blocking on send will pressure back to
                        // sender resulting in whacky behaviour+throughput
                        bounceBackExec.execute(new Runnable() {
                            @Override
                            public void run() {
                                while( ! backPub.offer(null,copy,0,len,true) ) {
                                    // spin
                                }
                                pool.returnBA(copy); // give back to pool
                            }
                        });
                    } else {
                        throw new RuntimeException("was soll das ?");
                    }
                }

                @Override
                public boolean dropped() {
                    // fatal, exit
                    System.out.println("process dropped ");
                    System.exit(-1);
                    return false;
                }

                @Override
                public void senderTerminated(String senderNodeId) {

                }

                @Override
                public void senderBootstrapped(String receivesFrom, long seqNo) {

                }
            }
        );
    }

    // just a simple test pool impl. Pretty inefficient, but don't want to introduce more dependencies on this
    // example project
    public static class StupidPool {
        volatile AtomicReferenceArray<byte[]> pool;

        public StupidPool(int size) {
            pool = new AtomicReferenceArray<byte[]>(size);
        }

        public void returnBA( byte ba[] ) {
            for ( int i=0; i < pool.length(); i++ ) {
                if ( pool.get(i) == null ) {
                    if (pool.compareAndSet(i,null,ba)) {
                        return;
                    }
                }
            }
        }

        static int success = 0;
        static int fail = 0;
        public byte[] getBA() {
            for ( int i=0; i < pool.length(); i++ ) {
                byte ba[] = pool.get(i);
                if ( ba != null ) {
                    if (pool.compareAndSet(i,ba,null)) {
                        success++;
                        return ba;
                    }
                }
            }
            fail++;
            if ( fail % 100000 == 0) {
                System.out.println("************* SUCC: "+success+" fail "+fail);
            }
            return new byte[1500]; // this is just a test impl with fixed size byte arrays
        }

    }

    public static void main(String arg[]) throws Throwable {
        AsyncSubStruct rec = new AsyncSubStruct();

        rec.initFastCast();
        while( true )
            Thread.sleep(10_000_000l);

    }

}
