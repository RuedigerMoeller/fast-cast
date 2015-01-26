package org.nustaq.fastcast.examples.latency.async;

import org.HdrHistogram.Histogram;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;
import org.nustaq.fastcast.util.RateMeasure;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ruedi on 23/01/15.
 *
 * This test measures asynchronous latency using multithreaded receiver processing.
 *
 * - Messages are sent from A to B
 * - B receives a message decodes it using serialization and puts it to a dedicated processing thread (see ObjectSubscriber class)
 * - Inside the processing thread, the same message is sent back from B to A
 * - A receives the message back, decodes it and computes the time in nanos
 *
 * So the following actions are measured:
 * - send message via network
 * - receive message from network
 * - decode message
 * - enqueue message to another thread
 * - [no processing] encode message again
 * - send message
 * - receive message
 * - decode message
 *
 * Note that this test uses fast-serialization for en/decoding. Using fst-structs will probably yield significant
 * better results (especially reduce GC related outliers)
 *
 * note: in order to test this on localhost, reduce the localhost mtu to 1550 bytes, else latency might get very high
 *
 * $ sudo ifconfig lo mtu 1550
 *
 * yields 25 microseconds rtt at 30k events per second
 *
 * note 1: pps setting determines the point batching starts. On a decent system/OS a pps of 50_000 is not a problem,
 * however many systems have built in limits (e.g. some windows cap at 25k on localhost). As batching trades latency for throughput
 * the higher pps, the lower the latency but also troughput
 *
 */
public class AsyncLatPublisher {

    public static final String CFG_FILE_PATH = "src/main/java/org/nustaq/fastcast/examples/latency/async/fc.kson";

    FastCast fastCast;
    ObjectPublisher pub;
    Histogram hi = new Histogram(TimeUnit.SECONDS.toNanos(2),3);
    Executor dumper = Executors.newCachedThreadPool();

    public void initFastCast() throws Exception {
        fastCast =  FastCast.getFastCast();
        fastCast.setNodeId("PUB");
        fastCast.loadConfig(CFG_FILE_PATH);

        pub = new ObjectPublisher(
            fastCast.onTransport("default").publish("stream"),
            AsyncLatMessage.class
        );

        fastCast.onTransport("back").subscribe( "back",
                new ObjectSubscriber(false,AsyncLatMessage.class) {
                    @Override
                    protected void objectReceived(String s, long l, Object o) {
                        if ( "END".equals(o) ) {
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
//                        hi.reset();
                            return;
                        }
                        final long value = System.nanoTime() - ((AsyncLatMessage) o).getSendTimeStampNanos();
                        if ( value < 1_000_000_000 )
                            hi.recordValue(value);
                    }

                    @Override
                    public boolean dropped() {
                        System.exit(-2);
                        return false;
                    }
                });

    }

    public void run( int pauseNanos, int numEvents ) throws Throwable {
        RateMeasure report = new RateMeasure("send rate");
        AsyncLatMessage event = new AsyncLatMessage(System.nanoTime(), 0, 0+1, 10, 110);
//        int msgCount = 0;
        for ( int i = 0; i < numEvents; i++ ) {
            double bidPrc = Math.random() * 10;
            event.setBidPrc(bidPrc);
            event.setAskPrc(bidPrc+1);
            event.setSendTimeStampNanos(System.nanoTime());
            pub.sendObject( null, event, true );
            report.count();
            long time = System.nanoTime();
            while( System.nanoTime() - time < pauseNanos ) {
                // spin
            }
//            msgCount++;
//            if ( (msgCount%10000) == 0 ) {
//                System.out.println("count "+msgCount);
//            }
        }
        pub.sendObject(null,"END", true);
    }

    public static void main(String arg[]) throws Throwable {
        AsyncLatPublisher pub = new AsyncLatPublisher();

        pub.initFastCast();
        while (true)
            pub.run( 5000, 1_000_000 ); // 93_000 = 10k, 27_000 = 30k, 10_500 = 70k, 4_900 = 140k

    }
}
