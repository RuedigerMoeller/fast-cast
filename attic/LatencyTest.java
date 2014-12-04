package de.ruedigermoeller.fastcast.test;

import org.nustaq.fastcast.api.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/6/13
 * Time: 5:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class LatencyTest {

    public static class LatencyService extends FCTopicService {

        volatile boolean stop = false;
        long startTime = System.currentTimeMillis();
        LatencyService remote;

        @Override
        public void init() {
            remote = (LatencyService) getRemoting().getRemoteService(getTopicName());
            remote.myStartTime(startTime);
            new Thread("Pinger") {
                @Override
                public void run() {
                    int count = 0;
                    while( ! stop ) {
                        try {
                            if ( count % 3 == 0 )
                                Thread.sleep(1);
                            count++;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        remote.unreliablePing(System.nanoTime());
                    }
                }
            }
            .start();
        }

        @RemoteMethod(1)
        public void roundTrip( long timeNanos, FCFutureResultHandler res ) {
            res.sendResult(timeNanos);
        }

        @RemoteMethod(3)
        public void myStartTime( long otherStartTime ) {
            System.out.println("Received myStartTime "+otherStartTime);
            if (startTime > otherStartTime ) {
                System.out.println("----- STOPPING ");
                stop = true;
            }
            else {
                System.out.println("----- ANSWERING ");
                remote.myStartTime(startTime);
            }
        }

        int cnt = 0;
        @RemoteMethod(2)
        public void unreliablePing( long timeSent ) {
            if (cnt++%1000 == 0) {
                if ( FCReceiveContext.get().getSender().equals(nodeId) ) {
                    System.out.println("Loopback ping received "+(System.nanoTime()-timeSent)/1000);
                } else {
                    System.out.println("ping received from "+FCReceiveContext.get().getSender()+" "+(System.nanoTime()-timeSent)/1000);
                }
            }
        }

    }

    public static void main(String arg[]) throws IOException, InterruptedException {
        FCRemoting fc = FastCast.getFastCast();
        fc.joinCluster("test/latency.yaml", "LatTest", null);
        fc.startSending("latency");
        fc.startReceiving("latency");
    }
}
