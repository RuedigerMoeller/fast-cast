package basic;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.bytesource.AsciiStringByteSource;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 10.12.14.
 */
public class TestEchoServer {

    public static void echoServer(FastCast fc) throws InterruptedException {

        final FCPublisher echoresp = fc.onTransport("default").publish(fc.getPublisherConf("echoresp"));
        final Executor responseExec = Executors.newSingleThreadExecutor();

        fc.onTransport("default").subscribe(fc.getSubscriberConf("echo"), new FCSubscriber() {

            @Override
            public void messageReceived(final String sender, long sequence, Bytez b, long off, int len) {
                // need to copy message as its valid only during the callback
                final byte[] bytes = b.toBytes(off, len); // could be optimized for reuse, just test
                responseExec.execute(new Runnable() {
                    @Override
                    public void run() {
                        while( ! echoresp.offer( sender, bytes,0, bytes.length, false) ) {
                            echoresp.flush();
                        }
                    }
                });
            }

            @Override
            public boolean dropped() {
                System.out.println("echoserver terminated !! FATAL ERROR. Enlarge send history");
                System.exit(0);
                return true;
            }

            @Override
            public void senderTerminated(String senderNodeId) {
                System.out.println(senderNodeId+" terminated");
            }

            @Override
            public void senderBootstrapped(String receivesFrom, long seqNo) {
                System.out.println("bootstrap "+receivesFrom);
            }
        });
        while( true ) {
            Thread.sleep(1000);
        }
    }

    public static void main(String arg[]) throws InterruptedException {
        FastCast fc = SendReceive.initFC("echo", "sendreceive.kson");
        echoServer(fc);
    }
}
