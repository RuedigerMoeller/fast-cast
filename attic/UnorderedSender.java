package de.ruedigermoeller.fastcast.test;

import org.nustaq.fastcast.api.FCRemoting;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.Unordered;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 19.08.13
 * Time: 13:26
 * To change this template use File | Settings | File Templates.
 */
@Unordered
public class UnorderedSender {

    String products[] = { "ALV", "BMW", "ODAX", "FDAX", "ZUAN", "OGBL" };
    int sequences[] = new int[products.length];
    private UnorderedReceiver remote;
    static FCRemoting fc;

    public UnorderedSender(int stream) {
        try {
            if ( fc == null ) {
                fc = FastCast.getFastCast();
                fc.joinCluster("test/unordered.yaml", "Bench", null);
            }
            String topic = "stream_" + stream;
            fc.startSending(topic);
            remote = (UnorderedReceiver) fc.getRemoteService(topic);
            sendLoop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendLoop() {
        new Thread("sendloop") {
            public void run () {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                while (true) {
                    sendStep();
                }
            }
        }.start();
    }

    private void sendStep() {
        for (int i = 0; i < products.length; i++) {
            String product = products[i];
            sequences[i]++;
            remote.receiveMarketData(product, sequences[i], 10 + (Math.random() * 6 - 3), 1000 * (int) (Math.random() * 10 + 1));
        }
    }

    public static void main( String arg[] ) throws IOException {
        new UnorderedSender(0);
        new UnorderedSender(1);
    }

}