package basic;

import org.junit.Test;
import org.nustaq.fastcast.config.ClusterConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {

    public static final String IFAC = "lo";

    public static class TestMsg extends FSTStruct {

        protected StructString string = new StructString(15);
        protected long timeNanos;

        public StructString getString() {
            return string;
        }

        public void setString(StructString string) {
            this.string = string;
        }

        public long getTimeNanos() {
            return timeNanos;
        }

        public void setTimeNanos(long timeNanos) {
            this.timeNanos = timeNanos;
        }
    }

    @Test
    public void send() throws InterruptedException {

        FastCast fc = initFC();

        TestMsg template = new TestMsg();
        FSTStructAllocator allocator = new FSTStructAllocator(0);

        TestMsg toSend = allocator.newStruct(template);

        FCPublisher sender = fc.getTransportDriver("default").publish(fc.getPublisherConf("test"));

        toSend.getString().setString("Hello");
        Sleeper sl = new Sleeper();
        RateMeasure measure = new RateMeasure("msg send");
        while( true ) {
//            Thread.sleep(500);
//            sl.sleepMicros(5);
            toSend.setTimeNanos(System.nanoTime());
            while ( ! sender.offer( toSend.getBase(), toSend.getOffset(), toSend.getByteSize(), false ) ) {
                System.out.println("offer rejected !");
            }
            measure.count();
//            System.out.println("sent msg");
        }
    }

    protected FastCast initFC() {
        System.setProperty("java.net.preferIPv4Stack","true" );
        FSTStructFactory.getInstance().registerClz(TestMsg.class);

        try {
            FastCast fc = FastCast.getFastCast();
            fc.loadConfig("/home/ruedi/IdeaProjects/fast-cast/src/test/java/basic/sendreceive.kson");
            return fc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void receive() throws InterruptedException {

        FastCast fc = initFC();

        final Executor worker = Executors.newSingleThreadExecutor();

        fc.getTransportDriver("default").subscribe(fc.getSubscriberConf("test"), new FCSubscriber() {

            int count = 0;

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                TestMsg received = FSTStructFactory.getInstance().getStructPointer(b, off).cast();
                final long nanos = System.nanoTime() - received.getTimeNanos();
                if (count++ % 1000 == 0) {
                    final TestMsg finRec = received.detach();
                    worker.execute(new Runnable() {
                        @Override
                        public void run() {
                            System.out.println("receive " + finRec.getString().toString() + " latency:" + (nanos / 1000));
                        }
                    });
                }
            }

            @Override
            public boolean dropped() {
                System.out.println("receiver dropped");
//                System.exit(1);
                return true;
            }

            @Override
            public void senderTerminated(String senderNodeId) {
                System.out.println("sender terminated " + senderNodeId);
            }

            @Override
            public void senderBootstrapped(String receivesFrom, long seqNo) {
                System.out.println("synced " + receivesFrom + " sequence " + seqNo);
            }

        });
        Thread.sleep(1000 * 1000);
    }
}
