package de.ruedigermoeller.fastcast.bigtest;

import de.ruedigermoeller.fastcast.bigtest.services.HTHost;
import de.ruedigermoeller.fastcast.bigtest.services.HTListener;
import org.nustaq.fastcast.remoting.FCFutureResultHandler;
import org.nustaq.fastcast.remoting.FCRemoting;
import org.nustaq.fastcast.remoting.FastCast;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/17/13
 * Time: 7:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableDataQuery {
    HTHost remoteHTHost;
    HTListener localHTListener;


    public TableDataQuery() {
    }

    public void start() {
        FCRemoting rem = FastCast.getFastCast();
        try {
            rem.joinCluster("shared/bigtest.yaml", "TQuery", null);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // just talk remotely to those (no local instance)
        remoteHTHost = (HTHost) rem.startSending("hthost");
        localHTListener = (HTListener) rem.getService("htlisten");
        localHTListener.setListener(new HTListener.DataListener() {
            @Override
            public void changeReceived(int id, Object prev, Object newVal) {
                lastRec = System.currentTimeMillis();;
            }
        });
        rem.startReceiving("htlisten");

        System.out.println("started "+rem.getNodeId());
        new Thread("waitForTest") {
            public void run() {
                while( true ) {
                    long now = System.currentTimeMillis();
                    if ( lastRec != 0 && now-lastRec > 5000 ) {
                        test.release();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            }
        }.start();
        processingLoop();
    }

    int responseCount;
    long lastRec = 0;
    Semaphore test = new Semaphore(0);
    String strSed[] = { "one", "two", "three", "four" };
    private void processingLoop() {
        //while( true )
        {
            try {
                responseCount = 0;
                final long tim = System.currentTimeMillis();
                test.acquire();
                System.out.println("startInq");
                remoteHTHost.getTableData(new FCFutureResultHandler() {
                    long lastRec = 0;
                    ConcurrentHashMap res = new ConcurrentHashMap();

                    @Override
                    public void timeoutReached() {
                        System.out.println("finshed "+responseCount+" in "+(lastRec-tim)+" ms. res size "+res.size());
                        ConcurrentHashMap mirror = localHTListener.getMirror();
                        System.out.println("mirror/res siz "+mirror.size()+" "+res.size());
                        for (Iterator iterator = mirror.keySet().iterator(); iterator.hasNext(); ) {
                            Object next = iterator.next();
                            if ( !Objects.deepEquals(mirror.get(next),res.get(next)) ) {
                                System.out.println("1st error at "+next);
                                System.out.println("  "+mirror.get(next)+" "+res.get(next));
                                break;
                            }
                        }
                        System.out.println("comparision done");
                    }

                    @Override
                    public void resultReceived(Object obj, String sender) {
                        responseCount++;
                        lastRec = System.currentTimeMillis();
                        Object val[] = (Object[]) obj;
                        res.put(val[0],val[1]);
                        extendTimeout();
                    }
                });

            } catch (InterruptedException e) {

            }
        }
    }

    public static void main( String arg[] ) throws IOException {
        TableDataQuery clusterClientNode = new TableDataQuery();
        clusterClientNode.start();
    }

}