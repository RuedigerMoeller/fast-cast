package basic.stuff;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.api.util.ObjectSubscriber;

import java.util.Date;

/**
 * Created by moelrue on 02.11.2015.
 */
public class Issue4_Slow {

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
            FCSubscriber sub = new ObjectSubscriber() {
                int count = 0;

                @Override
                protected void objectReceived(String sender, long sequence, Object msg) {
                    if ( msg instanceof String ) {
                        System.out.println(fc.getNodeId()+" received: "+count);
                        count = 0;
                    } else {
                        Object i_time[] = (Object[]) msg;
                        System.out.println( fc.getNodeId()+" received "+i_time[0]+" delay: "+(System.currentTimeMillis()-(Long)i_time[1]));
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
            FCPublisher rawPublisher = fc.onTransport("default").publish(fc.getPublisherConf("sendreceive"));
            publisher = new ObjectPublisher(rawPublisher);
        }
    }

    private void run() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                initFC();
                while( true ) {
                    for (int i = 0; i < 10; i++) {
                        try {
                            Thread.sleep((long) (Math.random()*5000));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        long now = System.currentTimeMillis();
                        System.out.println(fc.getNodeId()+" sending "+i+" "+ now);
                        publisher.sendObject(null,new Object[] { i, now}, true);
                    }
                    publisher.sendObject(null,"ten", true);
                }
            }
        }).start();
    }


    public static void main(String[] args) {
        Issue4_Slow i4 = new Issue4_Slow();
        i4.run();
        Issue4_Slow i4other = new Issue4_Slow(); // uncomment this for multi-process test
        i4other.run();
    }

}
