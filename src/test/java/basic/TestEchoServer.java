package basic;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.convenience.ObjectPublisher;
import org.nustaq.fastcast.convenience.ObjectSubscriber;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.bytesource.AsciiStringByteSource;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 10.12.14.
 *
 * Needs to be started before running tests. Uses very conservative rate limits, do not use for
 * benchmarking
 */
public class TestEchoServer {

    public static class UnreliableMessage implements Serializable {

        long sequence;

        public UnreliableMessage(long sequence) {
            this.sequence = sequence;
        }

        public long getSequence() {
            return sequence;
        }
    }

    public static class SampleBroadcast implements Serializable {

        String stringDate = new Date().toString();
        long aSecretNumber;

        public SampleBroadcast(long aSecretNumber) {
            this.aSecretNumber = aSecretNumber;
        }

        @Override
        public String toString() {
            return "SampleBroadcast{" +
                       "stringDate='" + stringDate + '\'' +
                       ", aSecretNumber=" + aSecretNumber +
                       '}';
        }

        public String getStringDate() {
            return stringDate;
        }

        public long getaSecretNumber() {
            return aSecretNumber;
        }
    }

    public static void echoServer(FastCast fc) throws InterruptedException {

        final FCPublisher echoresp = fc.onTransport("default").publish(fc.getPublisherConf("echoresp"));
        final Executor responseExec = Executors.newSingleThreadExecutor();
        final long startUpTime = (int) System.currentTimeMillis();

        startTestTopic(fc, startUpTime);
        startEchoTopic(fc, echoresp, responseExec);
        startUnreliableTopic(fc);

        while( true ) {
            Thread.sleep(1000);
        }
    }

    public static void startEchoTopic(FastCast fc, final FCPublisher echoresp, final Executor responseExec) {
        fc.onTransport("default").subscribe("echo", new FCSubscriber() {
            @Override
            public void messageReceived(final String sender, long sequence, Bytez b, long off, int len) {
                // need to copy message as its valid only during the callback
                final byte[] bytes = b.toBytes(off, len); // could be optimized for reuse, just test
                responseExec.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (!echoresp.offer(sender, bytes, 0, bytes.length, false)) {
//                            echoresp.flush();
                        }
//                        System.out.println("sent response to "+sender);
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
                System.out.println(senderNodeId + " terminated");
            }

            @Override
            public void senderBootstrapped(String receivesFrom, long seqNo) {
                System.out.println("bootstrap " + receivesFrom);
            }
        });
    }

    public static void startUnreliableTopic(FastCast fc) {
        fc.onTransport("default").subscribe( "unreliable", new ObjectSubscriber() {
            long lastSequence = 0;
            long gaps = 0;
            @Override
            protected void objectReceived(String sender, long sequence, Object msg) {
                UnreliableMessage umsg = (UnreliableMessage) msg;
                if ( umsg.getSequence() < lastSequence ) {
                    lastSequence = 0;
                    gaps = 0;
                }
                if (lastSequence > 0) {
                    if ( umsg.getSequence() != lastSequence+1 ) {
                        gaps++;
                    }
                }
                lastSequence = umsg.getSequence();
                if ( (lastSequence%100_000) == 0 ) {
                    System.out.println("received unreliable "+lastSequence+" gaps "+gaps);
                }
            }
        });
    }

    public static void startTestTopic(FastCast fc, final long startUpTime) {
        fc.onTransport("default").subscribe( "test", new ObjectSubscriber() {
            @Override
            protected void objectReceived(String sender, long sequence, Object msg) {
                System.out.println("received Object:"+msg);
            }
        });

        FCPublisher testPublisher = fc.onTransport("default").publish("test");
        final ObjectPublisher objectPublisher = new ObjectPublisher(testPublisher);
        new Thread("sender") {
            public void run() {
                while( true ) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    objectPublisher.sendObject(null,new SampleBroadcast(startUpTime),true);
                }
            }
        }.start();
    }

    public static void main(String arg[]) throws InterruptedException {
        FastCast fc = SendReceive.initFC("echo", "sendreceive.kson");
        echoServer(fc);
    }
}
