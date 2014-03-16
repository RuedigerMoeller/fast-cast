package de.ruedigermoeller.fastcast.samples.topicservice;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.config.FCConfigBuilder;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCLog;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 10/14/13
 * Time: 3:21 PM
 * To change this template use File | Settings | File Templates.
 *
 * To avoid boilerplate, no interface for the remote methods is defined. Also note that one
 * will probably use a separate Node main class setting up and managing several topics in case.
 */
public class ExampleTopicService extends FCTopicService {

    public static FCClusterConfig getClusterConfig() {
        // configure a cluster programatically (for larger scale apps one should prefer config files)
        FCClusterConfig conf = FCConfigBuilder.New()
                .socketTransport("default", "127.0.0.1", "229.9.9.9", 44444)
                  .topic("rpc", 0)
                  .membership("members",1) // built in topic for self monitoring+cluster view
                .end()
                .build();

        conf.setLogLevel(FCLog.INFO);
        try {
            // write out config to enable ClusterView
            new File("/tmp").mkdir(); // windoze ..
            conf.write("/tmp/conf.yaml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conf;
    }

    // empty constructor required for bytecode magic
    public ExampleTopicService() {

    }

    @Override
    public void init() {
        // called by FastCast prior to receiving the first message
    }

    @RemoteMethod(1)
    public void standardMethod( Object argument ) {
        System.out.println("standard Method "+argument);
    }

    @RemoteMethod(2)
    public void fastCall( int only, String simple, int types ) {
        System.out.println("fast call called ");
    }

    @RemoteMethod(3)
    public void withResult( long timeStamp, FCFutureResultHandler result ) {
        result.sendResult(timeStamp);
        System.out.println("sent result "+(System.currentTimeMillis()-timeStamp));
    }

    @RemoteMethod(5)
    public void multiResult( long timeStamp, FCFutureResultHandler result ) {
        result.sendResult(timeStamp);
    }

    // service implementation finished, code below should be moved to your applications
    // main class. Just all-in-one for demonstration purposes
    //////////////////////////////////////////////////////////////////////////////////////////////


    void initApp() throws Exception {
        FCRemoting remoting = FastCast.getRemoting();

        // always share same clusterconfig amongst all nodes
        FCClusterConfig conf = getClusterConfig();

        // wire sockets/queues etc.
        remoting.joinCluster(conf, "Receiver", null);

        // start receiving
        remoting.startReceiving("rpc", this);
    }

    public static void main( String arg[] ) throws Exception {
        ExampleTopicService node = new ExampleTopicService();
        node.initApp();
    }
}
