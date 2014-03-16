package de.ruedigermoeller.fastcast.test;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.config.FCConfigBuilder;
import de.ruedigermoeller.fastcast.packeting.TopicStats;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/8/13
 * Time: 5:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class ThroughputBench {


    //////////////////////////////////////////////////////////////////////////
    // sender stuff

    private final String type;
    private BenchService remote;

    public ThroughputBench(String type) {
        this.type = type;
    }

    void initCluster() {
        FCClusterConfig config = FCConfigBuilder.New()
                .socketTransport("socket", "127.0.0.1", "230.10.10.10", 37777)
                  .topic("bench", 1, 8000)
                .end()
                .loglevel( FCLog.INFO)
                .build();
        FCRemoting remoting = FastCast.getRemoting();

        try {

            remoting.joinCluster(config, "bench", "bn");

            if ( type != null ) // =>sender
            {
                remote = remoting.startSending("bench", BenchService.class);
                sendLoop();
            } else {
                BenchService benchService = new BenchService();
                remoting.startReceiving("bench", benchService);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    int sendCallCount;
    public void sendLoop() {
        byte some[] = {1,2,3,4,5,6,7,8,9};
        byte hundredB[] = new byte[100];
        byte twoHundredB[] = new byte[200];
        byte oneK[] = new byte[1000];
        byte sevenK[] = new byte[7000];
        byte sixtyFourK[] = new byte[65500];
        try {
            while( true ) {
                if ( type.equals("arr") ) {
                    remote.arrayTest(
                            new boolean[] { true, false, true, true, false },
                            new byte[] {2,3,4,5,6,7,-1,-2,-3 },
                            new char[] {2,3,4,5,6,7 },
                            new short[] {2,3,4,5,6,7, -1, -2, -3 },
                            new int[] {2,3,4,5,6,7, -1, -2, -3 },
                            new long[] {2,3,4,5,6,7, -1, -2, -3 },
                            new float[] {2,3,4,5,6,7, -1, -2, -3 },
                            new double[] {2,3,4,5,6,7, -1, -2, -3 }
                    );
                } else if ( type.equals( "bin") ) {
                    remote.receiveBinary(new HeapBytez(some),0,some.length);
                } else if ( type.equals("short") ) {
                    remote.shortMethod(sendCallCount);
                } else if ( type.equals("shortest") ) {
                    remote.shortestMethod((byte) 0);
                } else if ( type.equals("fat") ) {
                    HashMap hm = new HashMap();
                    hm.put( "Hallo", new Date() );
                    hm.put( 188, new Object[] { "any", "serializable", new Object[] {"content"} } );
                    remote.fatMethod( "fat", hm);
                } else if ( type.equals("some") ) {
                    remote.byteMethod(some);
                } else if ( type.equals("100") ) {
                    remote.byteMethod(hundredB);
                } else if ( type.equals("200") ) {
                    remote.byteMethod(twoHundredB);
                } else if ( type.equals("1k") ) {
                    remote.byteMethod(oneK);
                } else if ( type.equals("7k") ) {
                    remote.byteMethod(sevenK);
                } else if ( type.equals("64k") ) {
                    remote.byteMethod(sixtyFourK);
                } else throw new RuntimeException("unknown test mode");
                sendCallCount++;
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
            }
        } finally {
            System.out.println("uh-oh");
        }
    }

    /**
     * service impl (receiver)
     */
    public static class BenchService extends FCTopicService {

        int callCount, recCount;
        volatile long lastTime = System.currentTimeMillis();

        @Override
        public void init() {
        }


        @RemoteMethod(1)
        public void methodWithResult( long nanos, FCFutureResultHandler res ) {
            synchronized (this) {
                if ( System.currentTimeMillis() - lastTime > 1000 ) {
                    System.out.println("received/s "+recCount+" sent/replies:"+callCount);
                    recCount = 0;
                    callCount = 0;
                    lastTime = System.currentTimeMillis();
                }
            }
            res.sendResult(nanos);
            recCount++;
        }

        @RemoteMethod(2)
        public void byteMethod(byte b[]) {
            recCount++;
            if ( System.currentTimeMillis() - lastTime > 1000 ) {
                System.out.println("received/s "+recCount+" sent:"+callCount);
                recCount = 0;
                callCount = 0;
                lastTime = System.currentTimeMillis();
            }
        }

        @RemoteMethod(3)
        public void shortestMethod(byte b) {
            shortMethod(0);
        }

        int lastRec;

        TopicStats topicstats = new TopicStats(8000);

        @RemoteMethod(4)
        public void shortMethod(int i) {
            recCount++;
            dumpStats();
        }

        @RemoteMethod(5)
        public void arrayTest( boolean bool[], byte b[], char c[],short s[], int i[], long l[], float f[], double d[] ) {
            recCount++;
            dumpStats();
        }

        @RemoteMethod(6)
        public void fatMethod(String fat, HashMap thingy) {
            recCount++;
            dumpStats();
        }

        @Override
        @RemoteMethod(-1) // internal flag, do not use negative indizes in your code
        public void receiveBinary(Bytez bytes, int offset, int length) {
            recCount++;
            dumpStats();
        }

        private void dumpStats() {
            long l = System.currentTimeMillis();
            if (l - lastTime > 1000) {
                synchronized (this) {
                    if (l - lastTime > 1000) {
                        System.out.println("received/s " + recCount + " sent:" + callCount);
                        recCount = 0;
                        callCount = 0;
                        lastTime = System.currentTimeMillis();
                        TopicStats stats = getRemoting().getStats(getTopicName());
                        System.out.println("retrans:" + stats.getRetransVSDataPacketPercentage());
                        stats.addTo(topicstats, 10);
                        if ( System.currentTimeMillis()%100==0) {
                            System.out.println("###############################################################################");
                            System.out.println(topicstats);
                            System.out.println("-------------------------------------------------------------------------------");
                        }
                    }
                }
            }
        }
    }

    public static void main( String arg[] ) throws IOException {
        ThroughputBench throughputBench;
        if ( arg.length == 1 ) // => sender
        {
            throughputBench = new ThroughputBench(arg[0]);
        } else {
            throughputBench = new ThroughputBench(null);
        }
        throughputBench.initCluster();
    }


}
