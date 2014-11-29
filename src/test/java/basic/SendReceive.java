package basic;

import org.nustaq.fastcast.config.FCSubscriberConf;
import org.nustaq.fastcast.remoting.FCSubscriber;
import org.nustaq.fastcast.remoting.FastCast;
import org.nustaq.fastcast.transport.FCSocketConf;
import org.nustaq.offheap.bytez.ByteSource;
import org.nustaq.offheap.bytez.Bytez;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {

    public void send() {
        FastCast fc = FastCast.getFastCast();
        fc.createTransport(new FCSocketConf("default"));


    }

    public void receive() {
        System.setProperty("java.net.preferIPv4Stack","true" );
        FastCast fc = FastCast.getFastCast();
        fc.createTransport(new FCSocketConf("default"));
        fc.subscribe( new FCSubscriberConf("default",1), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, int off, int len)
            {
                System.out.println("receive bytes "+len);
            }

            @Override
            public void dropped() {
                System.out.println("receiver dropped");
            }

            @Override
            public void senderTerminated(String senderNodeId) {
                System.out.println("sender termniated "+senderNodeId);
            }

            @Override
            public void senderBootstrapped(String receivesFrom, long seqNo) {
                System.out.println("synced "+receivesFrom+" sequence "+seqNo );
            }
        });
    }
}
