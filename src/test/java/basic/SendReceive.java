package basic;

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.bytesource.AsciiStringByteSource;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {


    public static class TestMsg extends FSTStruct {

        protected StructString string = new StructString(1);
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

    protected FastCast initFC(String nodeId, String config) {
        System.setProperty("java.net.preferIPv4Stack","true" );
        FSTStructFactory.getInstance().registerClz(TestMsg.class);

        try {
            FastCast fc = FastCast.getFastCast();
            fc.setNodeId(nodeId);
            fc.loadConfig("/home/ruedi/IdeaProjects/fast-cast/src/test/java/basic/"+config);
//            fc.loadConfig("C:\\work\\GitHub\\fast-cast\\src\\test\\java\\basic\\sendreceive.kson");
            return fc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // echo test is for correctness not performance latency (unnecessary alloc etc.)
    @Test
    public void echoServer() throws InterruptedException {
        FastCast fc = initFC("echo","sendreceive.kson");
        final FCPublisher echoresp = fc.onTransport("default").publish(fc.getPublisherConf("echoresp"));

        fc.onTransport("default").subscribe(fc.getSubscriberConf("echo"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                String s = new String(b.toBytes(off, len), 0);
                while( ! echoresp.offer( sender, new AsciiStringByteSource("echo "+sender+" "+s), false) ) {
                }
            }

            @Override
            public boolean dropped() {
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

    @Test
    public void echoSend() throws InterruptedException {
        final FastCast fc = initFC("erec","sendreceive.kson");
        final FCPublisher echosend = fc.onTransport("default").publish(fc.getPublisherConf("echo"));

        fc.onTransport("default").subscribe(fc.getSubscriberConf("echoresp"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                System.out.println(fc.getNodeId()+" received "+new String(b.toBytes(off, len),0));
            }

            @Override
            public boolean dropped() {
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
            echosend.offer( null, new AsciiStringByteSource("hello"), true);
        }
    }

    @Test
    public void echoSendFragmenting() throws InterruptedException {
        String hello = "hello";
        // make it a 160k string
        while( hello.length() < 100_000 ) {
            hello += hello;
        }

        final FastCast fc = initFC("erec","sendreceive.kson");
        final FCPublisher echosend = fc.onTransport("default").publish(fc.getPublisherConf("echo"));

        fc.onTransport("default").subscribe(fc.getSubscriberConf("echoresp"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                String s = new String(b.toBytes(off, len), 0);
                System.out.println(fc.getNodeId()+" received '"+s.substring(0,20)+"' '"+s.substring(s.length()-20)+"' len:"+s.length());
                if ( s.length() != 163855 ) {
                    System.out.println("************************************************************************** bug len "+s.length());
                    System.out.println("  **************************************************************************");
                    System.out.println("  **************************************************************************");
                    System.out.println("  **************************************************************************");
                    System.out.println("  **************************************************************************");
                }
            }

            @Override
            public boolean dropped() {
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
            echosend.offer( null, new AsciiStringByteSource(hello), true);
        }
    }

    @Test
    public void send() throws InterruptedException {

        FastCast fc = initFC("SND","sendreceive.kson");

        TestMsg template = new TestMsg();
        FSTStructAllocator allocator = new FSTStructAllocator(0);

        TestMsg toSend = allocator.newStruct(template);

        FCPublisher sender = fc.onTransport("default").publish(fc.getPublisherConf("test"));

        toSend.getString().setString("H");
        Sleeper sl = new Sleeper();
        RateMeasure measure = new RateMeasure("msg send "+toSend.getByteSize());
        final Bytez base = toSend.getBase();
        final int byteSize = toSend.getByteSize();
        final long offset = toSend.getOffset();
        while( true ) {
            sl.sleepMicros(50);
            toSend.setTimeNanos(System.nanoTime());
            while ( ! sender.offer( null, base, offset, byteSize, true) ) {
//                System.out.println("offer rejected !");
            }
            measure.count();
//            System.out.println("sent msg");
        }
    }

    @Test
    public void testSleeper() {
        RateMeasure rm = new RateMeasure("ticks ps");
        Sleeper sl = new Sleeper();
        while( true ) {
            sl.sleepMicros(80);
            rm.count();
        }
    }

    @Test
    public void receive() throws InterruptedException {

        FastCast fc = initFC("REC","sendreceive.kson");

        final Histogram histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);

//        final Executor worker = Executors.newSingleThreadExecutor();

        fc.onTransport("default").subscribe( fc.getSubscriberConf("test"), new FCSubscriber() {

            int count = 0;
            int warmupCount = 0;

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                TestMsg received = FSTStructFactory.getInstance().getStructPointer(b, off).cast();
                final long nanos = System.nanoTime() - received.getTimeNanos();
                if ( warmupCount > 300 ) {
                    histo.recordValue(nanos);
                }
                if (count++ % 1000 == 0) {
                    warmupCount++;
                    if ( warmupCount > 500 ) {
                        warmupCount = 300;
                        histo.outputPercentileDistribution(System.out,1000.0);
                        histo.reset();
                    }
                    final TestMsg finRec = received.detach();
//                    worker.execute(new Runnable() {
//                        @Override
//                        public void run() {
//                        System.out.println("receive " + finRec.getString().toString() + " latency:" + (nanos / 1000));
//                        }
//                    });
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
