package bench;

import org.HdrHistogram.Histogram;
import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ruedi on 07.12.14.
 */
public class FCPing {

    public static final int PINGMSGLEN  = 256;
    public static final int NUM_MSG     = 1_000_000;

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

    protected FastCast initFC(String nodeId, String config) {
        System.setProperty("java.net.preferIPv4Stack","true" );
        FSTStructFactory.getInstance().registerClz(PingRequest.class);

        try {
            FastCast fc = FastCast.getFastCast();
            fc.setNodeId(nodeId);
            fc.loadConfig("/home/ruedi/IdeaProjects/fast-cast/src/test/java/bench/"+config);
            return fc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // no coordinated ommission, async
    public void pingClientAsnyc() throws InterruptedException {
        final FastCast fc = initFC("pclie", "pingponglat.kson");
        final FCPublisher pingserver = fc.onTransport("ping").publish(fc.getPublisherConf("pingtopic"));
        final Executor ex = Executors.newSingleThreadExecutor();
        final Histogram histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);
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
                    histo.outputPercentileDistribution(System.out, 1000.0);
                    histo.reset();
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
            sl.sleepMicros(100); // need rate limiting cause of async
            pr.setNanoSendTime(System.nanoTime());
            pingserver.offer( null, pr.getBase(), true); // can be sure off is 0, see (*)
        }
    }

    // CO !
    public void runPingClientSync() throws InterruptedException {
        final FastCast fc = initFC("pclie", "pingponglat.kson");
        final FCPublisher pingserver = fc.onTransport("ping").publish(fc.getPublisherConf("pingtopic"));
        final Executor ex = Executors.newSingleThreadExecutor();
        final AtomicInteger await = new AtomicInteger(0);

        fc.onTransport("pong").subscribe(fc.getSubscriberConf("pongtopic"), new FCSubscriber() {

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
        Histogram histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);

        Thread.sleep(1000); // wait for at least one heartbeat
        System.out.println("starting ping pong");
        System.gc();
        Sleeper sl = new Sleeper();
        while( true ) {
            await.set(0);
            for (int i= 0; i < NUM_MSG; i++ ) {
//                sl.sleepMicros(50);
                pingAndAwaitPong(pingserver, await, pr, histo, i);
            }
            histo.outputPercentileDistribution(System.out, 1000.0);
            histo.reset();
            System.gc();
        }
    }

    private void pingAndAwaitPong(FCPublisher pingserver, AtomicInteger await, PingRequest pr, Histogram histo, int i) {
        await.set(1);
        long tim = System.nanoTime();
        while( ! pingserver.offer( null, pr.getBase(), true) ) {
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
        if ( tim > 10_000_000 ) {
            System.out.println("peak "+tim+" at "+i);
        }
        histo.recordValue(tim);
    }

    public void runPongServer() throws InterruptedException {
        FastCast fc = initFC("pserv", "pingponglat.kson");
        final FCPublisher echoresp = fc.onTransport("pong").publish(fc.getPublisherConf("pongtopic"));

        fc.onTransport("ping").subscribe(fc.getSubscriberConf("pingtopic"), new FCSubscriber() {

            @Override
            public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                while( ! echoresp.offer( sender, b,off,len, true ) ) {
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

    public static void main(String arg[]) throws InterruptedException {
        new FCPing().runPingClientSync();
    }

}
