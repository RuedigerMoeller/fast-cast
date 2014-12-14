package basic;

import junit.framework.Assert;
import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.convenience.ObjectPublisher;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.fastcast.util.Sleeper;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.bytesource.AsciiStringByteSource;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by ruedi on 29.11.2014.
 */
public class SendReceive {

    public static FastCast initFC(String nodeId, String config) {
        System.setProperty("java.net.preferIPv4Stack","true" );
        FSTStructFactory.getInstance().registerClz(TestMsg.class);

        try {
            FastCast fc = FastCast.getFastCast();
            fc.setNodeId(nodeId);
            fc.loadConfig("./src/test/java/basic/"+config);
//            fc.loadConfig("C:\\work\\GitHub\\fast-cast\\src\\test\\java\\basic\\sendreceive.kson");
            return fc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static FastCast fc;
    static FCPublisher echosend;
    static AtomicReference response = new AtomicReference(null);

    public void initFC() {
        if ( fc == null ) {
            fc = initFC("erec","sendreceive.kson");
            echosend = fc.onTransport("default").publish(fc.getPublisherConf("echo"));
            fc.onTransport("default").subscribe(fc.getSubscriberConf("echoresp"), new FCSubscriber() {

                @Override
                public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
//                    System.out.println( "received from "+sender);
                    response.set(new String(b.toBytes(off,len),0));
                }

                @Override
                public boolean dropped() {
                    System.out.println("echoserver terminated !! FATAL ERROR. Enlarge send history");
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
            });
        }
    }

    public String sendReceiveSync( String msg ) throws InterruptedException {
        initFC();

        response.set(null);
        byte[] bytes = msg.getBytes();
        while( ! echosend.offer( null, bytes,0,bytes.length, true) ) {
            echosend.flush();
        }
        long tim = System.currentTimeMillis();
        while( response.get() == null ) {
            if ( System.currentTimeMillis()-tim > 30_000 )
                return null;
        }
        return (String) response.get();
    }

    @Test
    public void echoSendFragmenting() throws InterruptedException {
        initFC();
        String hello = "hello";
        // make it a 160k string
        while( hello.length() < 100_000 ) {
            hello += hello;
        }

        for ( int i = 0; i < 15; i++ ) {
            long tim = System.currentTimeMillis();
            String response = sendReceiveSync(hello);
            if ( response == null )
                response ="";
            Assert.assertTrue(response.equals(hello));
            long dur = System.currentTimeMillis() - tim;
            System.out.println("duration for " + hello.length() + " " + dur + " rate:" + (hello.length() / dur) + " kb/s");
            System.out.println("**yes**");
            Thread.sleep(1000);
        }
    }

    @Test
    public void sendUnreliable() throws InterruptedException {
        initFC();
        final FCPublisher publish = fc.onTransport("default").publish("unreliable");
        final ObjectPublisher objectPublisher = new ObjectPublisher(publish);
        for ( int i=0; i < 1_000_000; i++ ) {
            objectPublisher.sendObject(null,new TestEchoServer.UnreliableMessage(i),true);
        }
    }

    @Test
    public void echoSendFragmentingBig() throws InterruptedException {
        initFC();
        String hello = "hello";
        // make it a 5mb string
        while( hello.length() < 5*1_000_000 ) {
            hello += hello;
        }

        for ( int i = 0; i < 5; i++ ) {
            long tim = System.currentTimeMillis();
            String response = sendReceiveSync(hello);
            Assert.assertTrue(response.equals(hello));
            long dur = System.currentTimeMillis() - tim;
            System.out.println("******  duration for "+hello.length()+" "+dur+" rate:"+(hello.length()/dur)+" kb/s");
            System.out.println("***************************** yes **********************************");
            Thread.sleep(1000);
        }
    }

    @Test
    public void echoSendFragmentingHuge() throws InterruptedException {
        initFC();
        String hello = "hello";
        // make it a 40mb string
        while( hello.length() < 40*1_000_000 ) {
            hello += hello;
        }

        for ( int i = 0; i < 5; i++ ) {
            long tim = System.currentTimeMillis();
            String response = sendReceiveSync(hello);
            Assert.assertTrue(response.equals(hello));
            long dur = System.currentTimeMillis() - tim;
            System.out.println("******  duration for " + hello.length() + " " + dur + " rate:" + (hello.length() / dur) + " kb/s");
            System.out.println("***************************** yes **********************************");
            Thread.sleep(1000);
        }
    }

    @Test
    public void echoSendFragmentingSuperDuperHuge() throws InterruptedException {
        initFC();
        String hello = "hello";
        // make it a 100mb string
        while( hello.length() < 100*1_000_000 ) { // should be ~half receivebuffer size and < sendbuffer size
            hello += hello;
        }

        for ( int i = 0; i < 2; i++ ) {
            long tim = System.currentTimeMillis();
            String response = sendReceiveSync(hello);
            Assert.assertTrue(response.equals(hello));
            long dur = System.currentTimeMillis() - tim;
            System.out.println("******  duration for " + hello.length() + " " + dur + " rate:" + (hello.length() / dur) + " kb/s");
            System.out.println("***************************** yes **********************************");
            Thread.sleep(1000);
        }
    }

//    @Test
    public void echoSendFast() throws InterruptedException {
        initFC();
        for ( int i = 0; i < 1000; i++ ) {
            String hello = "hello" + i;
            String response = sendReceiveSync(hello);
            Assert.assertTrue(response.equals(hello));
        }
    }

//    @Test
    public void send() throws InterruptedException {

        initFC();
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
        long tim = System.currentTimeMillis();
        while( true ) {
            sl.sleepMicros(50);
            toSend.setTimeNanos(System.nanoTime());
            while ( ! sender.offer( null, base, offset, byteSize, true) ) {
//                System.out.println("offer rejected !");
            }
            measure.count();
//            System.out.println("sent msg");
            if ( System.currentTimeMillis() - tim > 5000 ) {
                break;
            }
        }
    }

    public void testSleeper() {
        RateMeasure rm = new RateMeasure("ticks ps");
        Sleeper sl = new Sleeper();
        while( true ) {
            sl.sleepMicros(80);
            rm.count();
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

}
