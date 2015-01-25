package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.util.RateMeasure;

/**
 * Created by ruedi on 24.01.2015.
 */
public class BatchingController {

    public static final int SIZE_OBSERVATION_WINDOW_TICKS = 4;

    public static enum Action {
        NONE,
        BATCH,
        BLOCK
    }

    public static final int RATE_PRECISION = 20;
    volatile static long MinResolution = 0;

    long lastCheckTS;

    int packetCounter;

    long nanosPerTick;
    long ticksPerSec;
    int packetsPerTick;

    int ratePerSecond; // unused, just memoized

    public BatchingController(int ratePerSecond) {
        this.ratePerSecond = ratePerSecond;
        if ( MinResolution == 0 ) {
            calibrate();
        }
        nanosPerTick = MinResolution;
        do {
            ticksPerSec = 1_000_000_000L / nanosPerTick;
            packetsPerTick = (int) (ratePerSecond / ticksPerSec);
            if (packetsPerTick < RATE_PRECISION) {
                nanosPerTick *=2;
            }
        } while ( packetsPerTick < RATE_PRECISION );
    }

    public int getRatePerSecond() {
        return ratePerSecond;
    }

    @Override
    public String toString() {
        return "BatchingController{" +
                "nanosPerTick=" + nanosPerTick +
                ", ticksPerSec=" + ticksPerSec +
                ", packetsPerTick=" + packetsPerTick +
                ", actualRate=" + packetsPerTick*ticksPerSec +
                '}';
    }

    private void calibrate() {
        long sum = 0;
        for ( int n = 0; n < 100; n++ ) {
            long nanos = System.nanoTime();
            while (System.nanoTime() == nanos) {
                // spin
            }
            long diff = System.nanoTime() - nanos;
//            System.out.println(diff);
            sum += diff;
        }
        MinResolution = Math.max(1, sum / 100);
        System.out.println("timer resolution:"+MinResolution+" ns");
    }

    public int getElapsedTicks() {
        final long now = System.nanoTime();
        if (lastCheckTS == 0) {
            lastCheckTS = now;
            return 0;
        }
        if (now - lastCheckTS > SIZE_OBSERVATION_WINDOW_TICKS *nanosPerTick ) {
            long elapsedTicks = (now-lastCheckTS) / nanosPerTick;
            lastCheckTS += nanosPerTick;
            packetCounter -= packetsPerTick;
            return 0;
        }
        return (int) ((now -lastCheckTS)/ nanosPerTick);
    }

    public int getAllowedPackets() {
        return getElapsedTicks()*packetsPerTick+1;
    }

    public void countPacket() {
        packetCounter++;
    }

    public Action getAction() {
        final int allowedPackets = getAllowedPackets();
        if ( packetCounter < allowedPackets ) {
            return Action.NONE;
        } else if ( packetCounter < allowedPackets + packetsPerTick*2 )
            return Action.BATCH;
        return Action.BLOCK;
    }

//
//    public void block() {
//        while ( packetCounter >= maxRatePerInterval) {
//            while( (System.nanoTime() - lastCheck) < interValNanos ) {
//                // spin
//            }
//            lastCheck = System.nanoTime();
//            packetCounter -= maxRatePerInterval;
//        }
//    }

    public static void main( String arg[] ) {

//        System.out.println(new BatchingController(100));
//        System.out.println(new BatchingController(200));
//        System.out.println(new BatchingController(1000));
//        System.out.println(new BatchingController(2000));
//        System.out.println(new BatchingController(5000));
//        System.out.println(new BatchingController(10000));
//        System.out.println(new BatchingController(15000));
//        System.out.println(new BatchingController(20000));
//        System.out.println(new BatchingController(30000));
//        System.out.println(new BatchingController(50000));
//        System.out.println(new BatchingController(70000));
//        System.out.println(new BatchingController(100000));
//        System.out.println(new BatchingController(500000));

        BatchingController limiter = new BatchingController(10000);
        RateMeasure m = new RateMeasure("msg rate");
        RateMeasure pm = new RateMeasure("packet rate");
        int msgPerPackAssumption = 4;
        int batchCount = 0;
        while( true ) {
//            LockSupport.parkNanos(1_000);
//            Sleeper.spinMicros(300);
            Action a = limiter.getAction();
            if ( a == Action.NONE ) {
                limiter.countPacket();
                pm.count();
                m.count();
            } else if ( a == Action.BATCH ) {
                if (batchCount == msgPerPackAssumption ) {
                    limiter.countPacket();
                    pm.count();
                    batchCount = 0;
                }
                batchCount++;
                m.count();
            } else /*block*/ {
                int x = 0;
            }
        }
    }
}
