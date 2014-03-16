package de.ruedigermoeller.fastcast.samples.binary;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.config.FCConfigBuilder;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/12/13
 * Time: 10:06 PM
 * To change this template use File | Settings | File Templates.
 */
// note this acts as sender and receiver ..
public class BinarySendReceiveNode {

    void init() throws Exception {
        FCRemoting remoting = FastCast.getRemoting();

        // configure a cluster programatically (for larger scale apps one should prefer config files)
        FCClusterConfig conf = FCConfigBuilder.New()
                .socketTransport("default", "127.0.0.1", "233.3.3.3", 33333)
                    .topic("binary", 0) // only one topic .. WARNING: rate limit is 8MB/s if not specified otherwise
                    .end()
                .build();

        conf.setLogLevel(FCLog.INFO);

        // wire sockets/queues and define name of this process in cluster
        remoting.joinCluster( conf,"BinNode", null );

        // subscribe to topic binary
        remoting.startReceiving("binary", new FCBinaryMessageListener() {
           @Override
           public void receiveBinary(Bytez bytes, int offset, int length) {
               System.out.println("received '"+new String(bytes.toBytes(offset,length),0,0,length)+"' sent By:"+FCReceiveContext.get().getSender());
           }
        });

        // start publishing ..
        FCBinaryTopicService sendProxy = (FCBinaryTopicService) remoting.startSending("binary", FCBinaryTopicService.class );
        // send loop
        while ( true ) {
           byte[] bytes = ("Hello from "+remoting.getNodeId()).getBytes();
           // send a binary message
           sendProxy.receiveBinary(new HeapBytez(bytes),0,bytes.length);

           // just send 1 messages a second ..
           Thread.sleep(1000);
           // note: default config is very conservative. You need to tune in order to get high throughput.
        }

    }

    public static void main( String arg[] ) throws Exception {
        BinarySendReceiveNode node = new BinarySendReceiveNode();
        node.init();
    }

}
