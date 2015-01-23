package org.nustaq.fastcast.examples.latency.async;

import org.HdrHistogram.Histogram;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;
import org.nustaq.fastcast.util.RateMeasure;

import java.util.concurrent.TimeUnit;

/**
 * Created by ruedi on 23/01/15.
 *
 * see AsyncLatPublisher for doc
 *
 */
public class AsyncLatReceiver {
    FastCast fastCast;
    Histogram hi = new Histogram(TimeUnit.SECONDS.toNanos(10),3);
    private ObjectPublisher backPub; // pong for rtt measurement

    public void initFastCast() throws Exception {
        fastCast =  FastCast.getFastCast();
        fastCast.setNodeId("SUB");
        fastCast.loadConfig(AsyncLatPublisher.CFG_FILE_PATH);

        backPub = new ObjectPublisher(
                fastCast.onTransport("back").publish("back"),
                AsyncLatMessage.class
        );

        final RateMeasure measure = new RateMeasure("receive rate");
        fastCast.onTransport("default").subscribe( "stream",
                new ObjectSubscriber(AsyncLatMessage.class) {
                    @Override
                    protected void objectReceived(String s, long l, Object o) {
                        if ( "END".equals(o) ) {
                            backPub.sendObject(null,o,true);
//                        hi.outputPercentileDistribution(System.out,1000.0);
//                        hi.reset();
                            return;
                        }
//                    hi.recordValue(System.nanoTime() - ((MeasuredEvent) o).getSendTimeStampNanos());
                        AsyncLatReceiver.this.objectReceived(s,l,o);
                        backPub.sendObject(null,o,true);
                        measure.count();
                    }
                    @Override
                    public boolean dropped() {
                        System.exit(-2);
                        return false;
                    }
                });
    }

    protected void objectReceived(String sender, long sequence, Object message) {
//        MarketEvent ev = (MarketEvent) message;
    }

    public static void main(String arg[]) throws Throwable {
        AsyncLatReceiver rec = new AsyncLatReceiver();

        rec.initFastCast();
        while( true )
            Thread.sleep(10_000_000l);

    }}
