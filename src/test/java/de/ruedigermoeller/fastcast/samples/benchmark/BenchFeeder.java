package de.ruedigermoeller.fastcast.samples.benchmark;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.remoting.FCFutureResultHandler;
import de.ruedigermoeller.fastcast.remoting.FCRemoting;
import de.ruedigermoeller.fastcast.remoting.FCSendContext;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.util.RateMeasure;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/15/13
 * Time: 9:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class BenchFeeder {

    private BenchTopicService sendBinProxy;

    void initApp() throws Exception {
        FCRemoting remoting = FastCast.getRemoting();

        // get the cluster config (only oone config per VM is possible)
        FCClusterConfig conf = BenchTopicService.getClusterConfig();

        // wire sockets/queues etc.
        remoting.joinCluster(conf, "Caller", null);

        // open topic for sending
        remoting.startSending("binbench", BenchTopicService.class );

        // get proxy object to call remote methods (=send messages to other nodes)
        sendBinProxy = (BenchTopicService) remoting.getRemoteService("binbench");
    }

    void sendBinary(byte b[], int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        while (System.currentTimeMillis()-start < durationSec) {
            for ( int i = 0; i < 1000; i++ )
                sendBinProxy.receiveBinary(new HeapBytez(b),0,b.length);

        }
    }

    void sendEmpty(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        while (System.currentTimeMillis()-start < durationSec) {
            for ( int i = 0; i < 1000; i++ )
                sendBinProxy.emptyCall();
        }
    }

    void sendFCInt(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        while (System.currentTimeMillis()-start < durationSec) {
            for ( int i = 0; i < 1000; i++ )
                sendBinProxy.fastCall(i);
        }
    }

    void sendFCIntString(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        while (System.currentTimeMillis()-start < durationSec) {
            for ( int i = 0; i < 1000; i++ )
                sendBinProxy.fastCall("String",i);
        }
    }

    void sendStdCall(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        BenchObject bo = new BenchObject(11);
        while (System.currentTimeMillis()-start < durationSec) {
            for ( int i = 0; i < 1000; i++ )
                sendBinProxy.standardCall(bo);
        }
    }

    void sendLowRandomTraffic(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        BenchObject bo = new BenchObject(11);
        while (System.currentTimeMillis()-start < durationSec) {
            sendBinProxy.randomCall(bo);
            try {
                Thread.sleep((long) (Math.random()*4)+1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void sendReqResp(int durationSec) {
        long start = System.currentTimeMillis();
        durationSec*=1000;
        BenchObject bo = new BenchObject(11);
        final RateMeasure reqResp = new RateMeasure("reqResp");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> res = new AtomicReference<>(null);
        sendBinProxy.reqRespCall(0, new FCFutureResultHandler() {
            @Override
            public void resultReceived(Object obj, String sender) {
                done();
                res.set(sender);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String reqReceiver = res.get();
        while (System.currentTimeMillis()-start < durationSec) {
            // target to random node, because in 1:N scenarios we would get overwhelmed (many reponses per request sent)
            FCSendContext.get().setReceiver(reqReceiver);
            sendBinProxy.reqRespCall(System.currentTimeMillis(), new FCFutureResultHandler() {
                @Override
                public void resultReceived(Object obj, String sender) {
                    done();
                    reqResp.count();
                }
            });
        }
    }

    /**
     * sample class send in bench
     */
    public static class BenchObject implements Serializable {
        public BenchObject(int index) {
            // avoid benchmarking identity references instead of StringPerf
            str = "Some Value "+index;
            str1 = "Very Other Value "+index;
            switch (index%3) {
                case 0: str2 = "Default Value"; break;
                case 1: str2 = "Other Default Value"; break;
                case 2: str2 = "Non-Default Value "+index; break;
            }
        }
        private String str;
        private String str1;
        private String str2;
        private boolean b0 = true;
        private boolean b1 = false;
        private boolean b2 = true;
        private int test1 = 123456;
        private int test2 = 234234;
        private int test3 = 456456;
        private int test4 = -234234344;
        private int test5 = -1;
        private int test6 = 0;
        private long l1 = -38457359987788345l;
        private long l2 = 0l;
        private double d = 122.33;
    }


    public static void main(String arg[]) throws Exception {
        BenchFeeder feeder = new BenchFeeder();
        feeder.initApp();

        while ( true ) {
            feeder.sendLowRandomTraffic(10);
            feeder.sendEmpty(30);
            feeder.sendLowRandomTraffic(10);
            feeder.sendFCInt(30);
            feeder.sendLowRandomTraffic(10);
            feeder.sendFCIntString(30);
            feeder.sendLowRandomTraffic(10);
            feeder.sendStdCall(30);
            feeder.sendLowRandomTraffic(10);
            feeder.sendReqResp(30);

            int[] byteMsgSizes = BenchTopicService.byteMsgSizes;
            for (int i = 1; i < byteMsgSizes.length; i++) {
                int byteMsgSize = byteMsgSizes[i];
                feeder.sendBinary(new byte[byteMsgSize],30);
            }
            feeder.sendLowRandomTraffic(10);
            feeder.sendEmpty(30);
        }

    }

}
