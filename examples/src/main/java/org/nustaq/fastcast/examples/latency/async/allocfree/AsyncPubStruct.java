package org.nustaq.fastcast.examples.latency.async.allocfree;

import org.HdrHistogram.Histogram;
import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;
import org.nustaq.fastcast.examples.latency.async.AsyncLatMessage;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.onheap.HeapBytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ruedi on 26/01/15.
 */
public class AsyncPubStruct {

    public static final String CFG_FILE_PATH = "src/main/java/org/nustaq/fastcast/examples/latency/async/fc.kson";

    FastCast fastCast;
    FCPublisher pub;
    Histogram hi = new Histogram(TimeUnit.SECONDS.toNanos(2),3);
    Executor dumper = Executors.newCachedThreadPool();

    public void initFastCast() throws Exception {
        fastCast =  FastCast.getFastCast();
        fastCast.setNodeId("PUB");
        fastCast.loadConfig(CFG_FILE_PATH);

        pub = fastCast.onTransport("default").publish("stream");

        // pointer to message
        final FSTStruct msg = FSTStructFactory.getInstance().createEmptyStructPointer(FSTStruct.class);

        fastCast.onTransport("back").subscribe( "back", new FCSubscriber() {
            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                // as structs decode literally in zero time, we can decode inside receiver thread
                msg.baseOn(b, (int) off);
                Class type = msg.getPointedClass();
                if ( type == StructString.class ) {
                    // sequence end, print histogram
                    final Histogram oldHi = hi;
                    hi = new Histogram(TimeUnit.SECONDS.toNanos(2),3);
                    // no lambdas to stay 1.7 compatible
                    // move printing out of the receiving thread
                    dumper.execute(new Runnable() {
                        @Override
                        public void run() {
                        oldHi.outputPercentileDistribution(System.out,1000.0);
                        }
                    });
                } else { // a regular message, record latency
                    AsyncLatMessageStruct mdata = msg.cast();
                    long value = System.nanoTime() - mdata.getSendTimeStampNanos();
                    if ( value < 1_000_000_000 ) { // avoid AIOB during init + JITTING
                        hi.recordValue(value);
                    }
                    // a real app would need to copy the message (recycle byte arrays/objects !) and
                    // run msg processing in an executor to get out of the receiving thread.
                    // mdata gets invalid on finish of this method
                }
            }

            @Override
            public boolean dropped() {
                return false;
            }

            @Override
            public void senderTerminated(String senderNodeId) {

            }

            @Override
            public void senderBootstrapped(String receivesFrom, long seqNo) {

            }
        });
    }

    public void run( int pauseNanos, int numEvents ) throws Throwable {
        RateMeasure report = new RateMeasure("send rate");
        // alloc + init a single instance which is reused then. if no allocator is used, default is OnHeap Alloc (struct is byte[] backed)
        AsyncLatMessageStruct event = (AsyncLatMessageStruct) new AsyncLatMessageStruct(System.nanoTime(), 0, 0+1, 10, 110).toOffHeap();
        byte underlying[] = event.getBase().asByteArray();
        byte[] end = new StructString("END").toOffHeap().getBase().asByteArray();
        for ( int i = 0; i < numEvents; i++ ) {
            double bidPrc = Math.random() * 10;
            event.setBidPrc(bidPrc);
            event.setAskPrc(bidPrc+1);
            event.setSendTimeStampNanos(System.nanoTime());
            while( ! pub.offer(null, underlying, 0, underlying.length, true) ) {
                // spin
            }
            report.count();
            long time = System.nanoTime();
            while( System.nanoTime() - time < pauseNanos ) {
                // spin
            }
        }
        while( ! pub.offer(null, end, 0, end.length, true) ) {
            // spin
        }
    }

    public static void main(String arg[]) throws Throwable {
        AsyncPubStruct pub = new AsyncPubStruct();

        pub.initFastCast();
        while (true)
            pub.run( 2000, 5_000_000 ); // 93_000 = 10k, 27_000 = 30k, 10_500 = 70k, 4_900 = 140k

    }

}
