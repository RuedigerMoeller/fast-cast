package basic.stuff;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;

/**
 * Created by moelrue on 02.11.2015.
 */
public class Issue4 {

    FastCast fc;
    ObjectPublisher publisher;

    public FastCast setupFC(String nodeId, String config) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        try {
            FastCast fc = new FastCast(); //FastCast.getFastCast();
            fc.setNodeId(nodeId);
            fc.loadConfig("./src/test/java/basic/"+config);
            return fc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void initFC() {
        if ( fc == null ) {

            fc = setupFC("t"+(int)(1000*Math.random()), "stuff/sendreceive.kson");

            FCPublisher rawPublisher = fc.onTransport("default").publish(fc.getPublisherConf("sendreceive"));
            publisher = new ObjectPublisher(rawPublisher);

            FCSubscriber sub = new ObjectSubscriber() {
                int count = 0;

                @Override
                protected void objectReceived(String sender, long sequence, Object msg) {
                    if ( msg instanceof String ) {
                        System.out.println(fc.getNodeId()+" received: "+count);
                        count = 0;
                    } else {
                        count++;
                    }
                }

                @Override
                public boolean dropped() {
                    System.out.println("FATAL ERROR. Enlarge send history");
                    System.exit(0);
                    return false;
                }

                @Override
                public void senderTerminated(String senderNodeId) {
                    System.out.println(senderNodeId+" terminated");
                }

                @Override
                public void senderBootstrapped(String receivesFrom, long seqNo) {
                    System.out.println("bootstrap "+receivesFrom);
                }
            };
            fc.onTransport("default").subscribe(fc.getSubscriberConf("sendreceive"), sub);
        }
    }

    private void run() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                initFC();
                while( true ) {
                    for (int i = 0; i < 1_000_000; i++) {
                        publisher.sendObject(null,i, false);
                    }
                    publisher.sendObject(null,"million", false);
                }
            }
        }).start();
    }


    public static void main(String[] args) {
        Issue4 i4 = new Issue4();
        i4.run();
//        Issue4 i4other = new Issue4(); // uncomment this for multi-process test
//        i4other.run();
    }

}
