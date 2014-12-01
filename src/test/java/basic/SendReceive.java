package basic;

import org.junit.Test;
import org.nustaq.fastcast.config.FCPublisherConf;
import org.nustaq.fastcast.config.FCSubscriberConf;
import org.nustaq.fastcast.remoting.FCPublisher;
import org.nustaq.fastcast.remoting.FCSubscriber;
import org.nustaq.fastcast.remoting.FastCast;
import org.nustaq.fastcast.config.FCSocketConf;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {

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

        initFactory();

        TestMsg template = new TestMsg();
        FSTStructAllocator allocator = new FSTStructAllocator(0);

        TestMsg toSend = allocator.newStruct(template);

        System.setProperty("java.net.preferIPv4Stack","true" );
        FastCast fc = FastCast.getFastCast();
        fc.createTransport(new FCSocketConf("default"));
        FCPublisher sender = fc.publish(new FCPublisherConf("default", 1));

        toSend.getString().setString("Hello");

        while( true ) {
            toSend.setTimeNanos(System.nanoTime());
            while ( ! sender.offer( toSend.getBase(), toSend.getOffset(), toSend.getByteSize()) ) {
                System.out.println("offer rejected !");
            }
//            LockSupport.parkNanos(1000*250);
//            System.out.println("sent");
        }
    }

    protected void initFactory() {
        FSTStructFactory.getInstance().registerClz(TestMsg.class);
    }

    @Test
    public void receive() throws InterruptedException {

        initFactory();

        Executor worker = Executors.newSingleThreadExecutor();

        System.setProperty("java.net.preferIPv4Stack","true" );
        FastCast fc = FastCast.getFastCast();
        fc.createTransport(new FCSocketConf("default"));
        fc.subscribe( new FCSubscriberConf("default",1), new FCSubscriber() {

            int count = 0;
            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len)
            {
                TestMsg received = FSTStructFactory.getInstance().createStructWrapper(b,off).cast();
                long nanos = System.nanoTime()-received.getTimeNanos();
                if ( count++%1000 == 0 ) {
                    worker.execute(() -> {
                        System.out.println("receive " + received.getString().toString() + " latency:" + (nanos / 1000));
                    });
                }
            }

            @Override
            public void dropped() {
                System.out.println("receiver dropped"); System.exit(1);
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
        Thread.sleep(1000*1000);
    }
}
