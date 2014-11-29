package de.ruedigermoeller.fastcast.bigtest;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/16/13
 * Time: 9:21 PM
 * To change this template use File | Settings | File Templates.
 */

import de.ruedigermoeller.fastcast.bigtest.services.HTHost;
import de.ruedigermoeller.fastcast.bigtest.services.HTListener;
import org.nustaq.fastcast.remoting.FCRemoting;
import org.nustaq.fastcast.remoting.FastCast;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * directly sends to HTHost and receives copy. not communication to ReqServer
 */
public class DataMutatingClient implements HTListener.DataListener {
    HTHost remoteHTHost;
    HTListener localHTListener;


    public DataMutatingClient() {
    }

    public void start() {
        FCRemoting rem = FastCast.getFastCast();
        try {
            rem.joinCluster("shared/bigtest.yaml", "MClient", null);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // just talk remotely to those (no local instance)
        remoteHTHost = (HTHost) rem.startSending("hthost");

        // install htlisten in this node
        rem.startReceiving("htlisten");
        localHTListener = (HTListener) rem.getService("htlisten");
        localHTListener.setListener(this);

        // let data host nodes assign modulo (~sharding)
        remoteHTHost.syncHostNumbers();

        System.out.println("started "+rem.getNodeId());

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        processingLoop();
    }

    int requestCount; long tim = System.currentTimeMillis(); int responseCount;
    byte toSend[] = new byte[100];
    String strSed[] = { "one", "two", "three", "four" };
    boolean bool = true;
    private void processingLoop() {
        while( bool ) {
            final int random = (int) (Math.random() * 1000000);
            requestCount++;
            remoteHTHost.putData(random,strSed);
        }
        int maxSend = 1000000;
        for (int random = 0; random < maxSend; random++) {
            requestCount++;
            remoteHTHost.putData(random,strSed);
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        System.out.println("***** loop finished");
        remoteHTHost.printTableSize();
        int count = 0;
        while( localHTListener.getMirror().size() < maxSend && count < 10) {
            System.out.println(" mirror size "+count+" s lag " + localHTListener.getMirror().size());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            count++;
        }
        int errCount = 0;
        for (int random = 0; errCount < 100 && random < maxSend; random++) {
            if ( localHTListener.getMirror().get(random) == null ) {
                System.out.println("Error at "+random);
                errCount++;
            }
        }
    }

    AtomicBoolean lock = new AtomicBoolean(false);
    @Override
    public void changeReceived(int id, Object prev, Object newVal) {
        responseCount++;
        if (lock.compareAndSet(false,true)) {
            long now = System.currentTimeMillis();
            if ( now - tim > 1000 ) {
                System.out.println(""+Thread.currentThread().getName()+" resp "+responseCount+" req:"+requestCount+ " in "+(now-tim)+" millis");
//                System.out.println("mirror size " + localHTListener.getMirror().size());
                requestCount = responseCount = 0;
                tim = now;
            }
            lock.set(false);
        }
    }


    public static void main( String arg[] ) throws IOException {
        DataMutatingClient clusterClientNode = new DataMutatingClient();
        clusterClientNode.start();
    }

}
