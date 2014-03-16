package de.ruedigermoeller.fastcast.samples.topicservice;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.remoting.*;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 10/14/13
 * Time: 3:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExampleTopicClient {

    ExampleTopicService sendProxy;

    void initApp() throws Exception {
        FCRemoting remoting = FastCast.getRemoting();

        // get the cluster config (only oone config per VM is possible)
        FCClusterConfig conf = ExampleTopicService.getClusterConfig();

        // write out config to enable ClusterView
        new File("/tmp").mkdir(); // windoze ..
        conf.write("/tmp/rpc.yaml");

        // wire sockets/queues etc.
        remoting.joinCluster(conf, "Caller", null);

        // open topic for sending
        remoting.startSending("rpc", ExampleTopicService.class );

        // get proxy object to call remote methods (=send messages to other nodes)
        sendProxy = (ExampleTopicService) remoting.getRemoteService("rpc");

    }

    private void sendLoop() throws InterruptedException {
        while ( true ) {
            // fire and forget messages, (no result)

            // methods with simple types (primitive and/or String) are optimized by fastcast
            sendProxy.fastCall(10, "hello", 123);
            // (note slow sending will prevent any JIT compilation, so expect weird roundtrip times)
            Thread.sleep(200);

            sendProxy.standardMethod( new Object[] { "Some serializable Object graph" } );
            Thread.sleep(200);

            // note this will receive a result from each receiver
            sendProxy.withResult(System.currentTimeMillis(), new FCFutureResultHandler() {
                @Override
                public void resultReceived(Object obj, String sender) {
                    Long result = (Long) obj;
                    System.out.println("receveived result, roundtrip time "+(System.currentTimeMillis()-result) );
                }
            });
            Thread.sleep(300);

            // Only take the result of fastest responder
            sendProxy.withResult(System.currentTimeMillis(), new FCFutureResultHandler() {
                @Override
                public void resultReceived(Object obj, String sender) {
                    done(); // not interested in more results. Important as callback entry can be deleted immediately => higher throughput
                    Long result = (Long) obj;
                    System.out.println("fastest "+ FCReceiveContext.get().getSender() +" "+(System.currentTimeMillis()-result));
                }
            });
            Thread.sleep(300);
        }
    }

    public static void main( String arg[] ) throws Exception {
        ExampleTopicClient node = new ExampleTopicClient();
        node.initApp();
        node.sendLoop();
    }
}
