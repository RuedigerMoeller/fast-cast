package basic;

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.config.ClusterConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.bytesource.AsciiStringByteSource;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {

    public static final String IFAC = "lo";
    public static final int PINGMSGLEN = 256;

    public static class PingRequest extends FSTStruct {
        protected long nanoSendTime;
        protected byte[] payload = new byte[PINGMSGLEN-8];

        public long getNanoSendTime() {
            return nanoSendTime;
        }

        public void setNanoSendTime(long nanoSendTime) {
            this.nanoSendTime = nanoSendTime;
        }

    }

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
        FSTStructFactory.getInstance().registerClz(TestMsg.class,PingRequest.class);

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

    @Test
    public void pongServer() throws InterruptedException {
        FastCast fc = initFC("pserv", "pingponglat.kson");
        final FCPublisher echoresp = fc.onTransport("pong").publish(fc.getPublisherConf("pong"));

        fc.onTransport("ping").subscribe(fc.getSubscriberConf("ping"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                echoresp.setReceiver(sender);
                while( ! echoresp.offer( b,off,len, true) ) {
                    echoresp.flush(); // ensure retrans processing etc.
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
            Thread.sleep(100000);
        }
    }

    Histogram histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);

    // no coordinated ommission
    @Test
    public void pingClient() throws InterruptedException {
        final FastCast fc = initFC("pclie", "pingponglat.kson");
        final FCPublisher pingserver = fc.onTransport("ping").publish(fc.getPublisherConf("ping"));
        final Executor ex = Executors.newSingleThreadExecutor();

        fc.onTransport("pong").subscribe(fc.getSubscriberConf("pong"), new FCSubscriber() {

            int msgCount;
            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                // decode bounced back ping request
                PingRequest received = FSTStructFactory.getInstance().getStructPointer(b, off).cast();
                if ( msgCount > 100_000) // filter out pressure of printing histogram
                    histo.recordValue(System.nanoTime()-received.getNanoSendTime());
                msgCount++;
                if ( msgCount > 1_100_000 ) {
                    final Histogram finH = histo;
                    histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);
                    ex.execute(new Runnable() {
                        @Override
                        public void run() {
                            finH.outputPercentileDistribution(System.out, 1000.0);
                        }
                    });
                    msgCount = 0;
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

        FSTStructAllocator alloc = new FSTStructAllocator(0); // just create a byte[] for each struct (*)
        PingRequest pr = alloc.newStruct( new PingRequest() );
        Sleeper sl = new Sleeper();
        while( true ) {
            sl.sleepMicros(100);
            pr.setNanoSendTime(System.nanoTime());
            pingserver.offer(pr.getBase(),true); // can be sure off is 0, see (*)
        }
    }

    // guilty of coordinated ommission
    @Test
    public void pingClientSync() throws InterruptedException {
        final FastCast fc = initFC("pclie", "pingponglat.kson");
        final FCPublisher pingserver = fc.onTransport("ping").publish(fc.getPublisherConf("ping"));
        final Executor ex = Executors.newSingleThreadExecutor();
        final AtomicInteger await = new AtomicInteger(0);

        fc.onTransport("pong").subscribe(fc.getSubscriberConf("pong"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                await.decrementAndGet();
            }

            @Override
            public boolean dropped() {
                await.set(0); // reset
                System.out.println("Drop and Reset counter !");
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

        FSTStructAllocator alloc = new FSTStructAllocator(0); // just create a byte[] for each struct (*)
        PingRequest pr = alloc.newStruct( new PingRequest() );

        Thread.sleep(1000); // wait for at least one heartbeat
        System.out.println("starting ping pong");
        Sleeper sl = new Sleeper();
        while( true ) {
            await.set(0);
            for (int i= 0; i < 1_000_000; i++ ) {
//                sl.sleepMicros(100);
                long tim = System.nanoTime();
                await.set(1);
                while( ! pingserver.offer(pr.getBase(),true) ) {
                }
                long waitTim = System.nanoTime();
                while( await.get() > 0 ) {
                    long delay = System.nanoTime() - waitTim;
                    if ( delay > 1000l*1000*1000*3 ) {
                        System.out.println("*** no response - give up ! ***");
                        break;
                    }
                }
                tim = System.nanoTime() - tim;
                histo.recordValue(tim);
            }
            histo.outputPercentileDistribution(System.out, 1000.0);
            histo.reset();
        }
    }

    // echo test is for correctness not performance latency (unnecessary alloc etc.)
    @Test
    public void echoServer() throws InterruptedException {
        FastCast fc = initFC("echo","sendreceive.kson");
        final FCPublisher echoresp = fc.onTransport("default").publish(fc.getPublisherConf("echoresp"));

        fc.onTransport("default").subscribe(fc.getSubscriberConf("echo"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                echoresp.setReceiver(sender);
                while( ! echoresp.offer(new AsciiStringByteSource("echo "+sender), false) ) {
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
            echosend.offer(new AsciiStringByteSource("hello"),true);
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
            while ( ! sender.offer(base, offset, byteSize, true ) ) {
//                System.out.println("offer rejected !");
            }
            measure.count();
//            System.out.println("sent msg");
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
