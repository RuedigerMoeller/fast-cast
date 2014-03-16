package de.ruedigermoeller.fastcast.bigtest;

import de.ruedigermoeller.fastcast.bigtest.services.HTHost;
import de.ruedigermoeller.fastcast.bigtest.services.HTListener;
import de.ruedigermoeller.fastcast.bigtest.services.RequestServer;
import de.ruedigermoeller.fastcast.remoting.FCFutureResultHandler;
import de.ruedigermoeller.fastcast.remoting.FCRemoting;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.service.FCMembership;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 5:08 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * sends request to requestprocessor, the takes the result and sends to HTHosts, receives mirror notification
 * from HTHosts.
 */
public class ClusterClientNode {

    RequestServer remoteServer;
    HTHost remoteHTHost;

    HTListener localHTListener;


    public ClusterClientNode() {
    }

    public void start() {
        FCRemoting rem = FastCast.getRemoting();
        try {
            rem.joinCluster("shared/bigtest.yaml", "Client", null);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // just talk remotely to those (no local instance)
        remoteServer = (RequestServer) rem.startSending("rqserver");
        remoteHTHost = (HTHost) rem.startSending("hthost");

        // install htlisten in this node
        rem.startReceiving("htlisten");
        localHTListener = (HTListener) rem.getService("htlisten");

        // let data host nodes assign modulo (~sharding)
        remoteHTHost.syncHostNumbers();

        System.out.println("started "+rem.getNodeId());
        processingLoop();
    }

    int responseCount; int requestCount; long tim = System.currentTimeMillis();
    private void processingLoop() {
        while( true ) {
            final int random = (int) (Math.random() * 1000000);
            requestCount++;
            remoteServer.receiveRequest(random, new FCFutureResultHandler() {
                @Override
                public void resultReceived(Object obj, String sender) {
                    done();
                    responseCount++;
                    if ( random % 10 == 0 ) {
                        long now = System.currentTimeMillis();
                        if ( now - tim > 1000 ) {
                            System.out.println(""+Thread.currentThread().getName()+" resp "+responseCount+" req:"+requestCount+ " in "+(now-tim)+" millis");
                            System.out.println(obj + " mirror size " + localHTListener.getMirror().size());
                            requestCount = responseCount = 0;
                            tim = now;
                            FCMembership ship = (FCMembership) FastCast.getRemoting().getService("membership");
                            System.out.println(ship.dumpToString());
                        }
                    }
                    remoteHTHost.putData(random,obj);
                }
            });
//            try {
//                Thread.sleep(0,5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public static void main( String arg[] ) throws IOException {
        ClusterClientNode clusterClientNode = new ClusterClientNode();
        clusterClientNode.start();
    }

}
