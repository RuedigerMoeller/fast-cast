package de.ruedigermoeller.fastcast.test;

import org.nustaq.fastcast.api.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 19.08.13
 * Time: 13:45
 * To change this template use File | Settings | File Templates.
 */
@Unordered
public class UnorderedReceiver extends FCTopicService {


    HashMap<String,Integer> sequences = new HashMap<String, Integer>();
    HashMap<String,Double> currentPrc = new HashMap<String, Double>();
    HashMap<String,Integer> currentQty = new HashMap<String, Integer>();

    int recCount = 0;
    int drop = 0;
    private long lastTime;

    @RemoteMethod(1)
    public void receiveMarketData(String product, int newSequence, double prc, int qty) {
        if ( product.length() > 4 ) {
            System.out.println("pok "+product);
        }
        Integer oldSequence = sequences.get(product);
        if (oldSequence==null) {
            sequences.put(product,newSequence);
        } else {
            if (newSequence<=oldSequence.intValue()) {
//                System.out.println("old detected "+product+" oldSequence "+newSequence+" received oldSequence "+oldSequence);
                return;
            }
            if ( newSequence != oldSequence.intValue()+1 ) {
//                System.out.println("drop detected "+product+" oldSequence "+newSequence+" received oldSequence "+oldSequence);
                drop++;
            }
        }
        recCount++;
        currentPrc.put(product,prc);
        currentQty.put(product,qty);
        sequences.put(product,newSequence);
        if ( System.currentTimeMillis() - lastTime > 1000 ) {
            System.out.println("received/s "+recCount+" dropped:"+drop+" "+getTopicName());
            recCount = 0;
            drop = 0;
            lastTime = System.currentTimeMillis();
        }
    }

    public static void main( String arg[] ) throws IOException {
        FCRemoting fc = FastCast.getFastCast();
        fc.joinCluster("test/unordered.yaml", "Bench", null);
        fc.startReceiving("stream_0");
        fc.startReceiving("stream_1");
    }

}
