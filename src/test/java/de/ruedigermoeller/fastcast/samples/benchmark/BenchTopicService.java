package de.ruedigermoeller.fastcast.samples.benchmark;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.config.FCConfigBuilder;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.fastcast.util.RateMeasure;
import de.ruedigermoeller.heapoff.bytez.Bytez;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/15/13
 * Time: 7:46 PM
 * To change this template use File | Settings | File Templates.
 */

@PerSenderThread(true) // methods can be multithreaded !
public class BenchTopicService extends FCTopicService {

    public static FCClusterConfig getClusterConfig() {
        // configure a cluster programatically (for larger scale apps one should prefer config files)
        int portBase = 55555;
        FCClusterConfig conf = FCConfigBuilder.New()
                .socketTransport("default_transport", "interface", "230.10.10.10", portBase)
                  .topic("binbench", 0, 20000, 5) // binary + fastcall
                          //~ 14.000 = 1 GBit network saturation
                          //~ 20.000-30.000 = Local host on newer hardware (i7,3.x GhZ)
                  .setRequestRespOptions(20000,2000)
                  .end()
                // run topic stats on different transport to reduce effect on benchmarking
                .socketTransport("stats_transport", "interface", "230.10.10.11", portBase + 1)
                  .membership("stats", 2)
                  .end()
                .build();

        // only one sharedmem transport per VM allowed on linux
//        FCClusterConfig conf = FCConfigBuilder.New()
//                .sharedMemTransport("default_transport",new File("/tmp/benchqueue.mem"),200,24000)
//                    .topic("binbench", 0, 25000, 2) // binary + fastcall
//                    .membership("stats",2)
//                    .end()
//                .build();


        // unsupported by builder ..
        conf.setLogLevel(FCLog.DEBUG);

//        conf.defineInterface("interface", "bond0"); // localhost
        conf.defineInterface("interface", "127.0.0.1"); // localhost

        conf.overrideBy("exbench.yaml");

        try {
            // write out config to enable ClusterView
            new File("/tmp").mkdir(); // windoze ..
            conf.write("/tmp/bench.yaml");

        } catch (IOException e) {
            e.printStackTrace();
        }

//        conf.defineInterface("interface", "eth0"); // centipede
//        conf.defineInterface("interface", "eth1"); // zerotwin, onetwin

        return conf;
    }

    public BenchTopicService() {
    }

    public static int byteMsgSizes[] = new int[] {
            0, 1, 10, 100, 1000,
            10000,
            100000,
            1000000
    };
    RateMeasure binaryMeasure[];

    void initBinaryMeasure() {
        binaryMeasure = new RateMeasure[byteMsgSizes.length];
        for (int i = 0; i < byteMsgSizes.length; i++) {
            int byteMsgSize = byteMsgSizes[i];
            String str = ""+byteMsgSize;
            if ( byteMsgSize >= 1000 ) {
                str = (byteMsgSize/1000)+"k";
            }
            binaryMeasure[i] = new RateMeasure("binary "+str);
        }
    }

    @Override
    @RemoteMethod(-1) // special for binary
    public void receiveBinary(Bytez bytes, int offset, int length) {
        for ( int i = byteMsgSizes.length-1; i >= 0; i-- ) {
            if (length > byteMsgSizes[i] ) {
                binaryMeasure[i+1].count();
                return;
            }
        }
    }

    RateMeasure emptyCall = new RateMeasure("method()");
    @RemoteMethod(1)
    public void emptyCall() {
        emptyCall.count();
    }

    RateMeasure fastCallInt = new RateMeasure("method(int)");
    @RemoteMethod(2)
    public void fastCall(int i) {
        fastCallInt.count();
    }

    RateMeasure fastCallStrInt = new RateMeasure("method(String,int)");
    @RemoteMethod(3)
    public void fastCall(String s, int i) {
        fastCallStrInt.count();
    }

    RateMeasure standardCallObj = new RateMeasure("method( BenchObject )");
    @RemoteMethod(4)
    public void standardCall(Object obj) {
        standardCallObj.count();
    }

    RateMeasure randomCallObj = new RateMeasure("random traffic");
    @RemoteMethod(5)
    public void randomCall(Object obj) {
        randomCallObj.count();
    }

    RateMeasure reqResp = new RateMeasure("req resp (long)");
    @RemoteMethod(6)
    public void reqRespCall(long tim, FCFutureResultHandler res) {
        reqResp.count();
        res.sendResult(tim);
    }

    public static void main(String arg[]) throws Exception {
        FastCast.getRemoting().joinCluster(getClusterConfig(),"benRec", null);
        BenchTopicService topicService = new BenchTopicService();
        BenchTopicService slowService = new BenchTopicService();
        topicService.initBinaryMeasure();
        FastCast.getRemoting().startReceiving("binbench", topicService);
    }
}
