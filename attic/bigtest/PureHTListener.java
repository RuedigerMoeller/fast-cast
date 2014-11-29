package de.ruedigermoeller.fastcast.bigtest;

import de.ruedigermoeller.fastcast.bigtest.services.HTListener;
import org.nustaq.fastcast.remoting.FCRemoting;
import org.nustaq.fastcast.remoting.FastCast;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/16/13
 * Time: 9:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class PureHTListener implements HTListener.DataListener {
    HTListener localHTListener;

    public PureHTListener() {
    }

    public void start() {
        FCRemoting rem = FastCast.getFastCast();
        try {
            rem.joinCluster("shared/bigtest.yaml", "Listen", null);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // install htlisten in this node
        rem.startReceiving("htlisten");
        localHTListener = (HTListener) rem.getService("htlisten");
        localHTListener.setListener(this);

        System.out.println("started "+rem.getNodeId());
    }

    long tim = System.currentTimeMillis();
    int responseCount;

    @Override
    public void changeReceived(int id, Object prev, Object newVal) {
        responseCount++;
        if ( id % 10 == 0 ) {
            synchronized (this) {
                long now = System.currentTimeMillis();
                if ( now - tim > 1000 ) {
                    System.out.println(""+Thread.currentThread().getName()+" resp "+responseCount+" in "+(now-tim)+" millis");
                    System.out.println("mirror size " + localHTListener.getMirror().size());
                    tim = now;
                    responseCount = 0;
                }
            }
        }
    }


    public static void main( String arg[] ) throws IOException {
        PureHTListener clusterClientNode = new PureHTListener();
        clusterClientNode.start();
    }
}