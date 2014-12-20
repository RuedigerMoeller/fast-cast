package org.nustaq.fastcast.examples.multiplestructs;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;

import java.util.concurrent.ThreadLocalRandom;
import static org.nustaq.fastcast.examples.multiplestructs.MultipleProtocol.*;

/**
 * Created by ruedi on 17.12.14.
 */
public class MPPublisher {

        public static void main(String arg[]) {

        FastCast.getFastCast().setNodeId("MPUB"); // 5 chars MAX !!
        configureFastCast();

        FCPublisher pub = FastCast.getFastCast().onTransport("default").publish(
                new PublisherConf(1)            // unique-per-transport topic id
                    .numPacketHistory(33_000)   // how many packets are kept for retransmission requests
                    .pps(10_000)                // packets per second rate limit.
        );

        MultipleProtocol.initStructFactory();
        FSTStructAllocator onHeapAlloc = new FSTStructAllocator(10_000);

        AMessage aMsg = onHeapAlloc.newStruct(new AMessage());
        OtherMessage other = onHeapAlloc.newStruct(new OtherMessage());
        ComposedMessage composed = onHeapAlloc.newStruct(new ComposedMessage());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // could directly send raw on publisher
        RateMeasure measure = new RateMeasure("msg/s");
        int count = 0;
        FSTStruct msg = null;

        while( true ) {

            measure.count();

            // fill in data
            switch( random.nextInt(3) ) {
                case 0:
                    for ( int i = 0; i < aMsg.stringArrayLen(); i++ ) {
                        aMsg.stringArray(i).setString("Hello "+i);
                    }
                    aMsg.setL(count++);
                    msg = aMsg;
                    break;
                case 1:
                    other.setQuantity(count++);
                    other.setValue(random.nextDouble());
                    msg = other;
                    break;
                case 2:
                    aMsg.stringArray(0).setString("Hello !");
                    aMsg.stringArray(1).setString("Hello !!");
                    aMsg.setL(count++);
                    other.setQuantity(count++);
                    other.setValue(random.nextDouble());

                    composed.setMegA(aMsg); // does a copy !
                    composed.setMsgB(other); // does a copy !
                    msg = composed;
                    break;
            }
            // send message
            while( ! pub.offer(null,msg.getBase(),msg.getOffset(),msg.getByteSize(),false) ) {
                /* spin */
            }

        }
    }

    public static void configureFastCast() {
        // note this configuration is far below possible limits regarding throughput and rate
        FastCast fc = FastCast.getFastCast();
        fc.addTransport(
                new PhysicalTransportConf("default")
                        .interfaceAdr("127.0.0.1")  // define the interface
                        .port(42044)                // port is more important than address as some OS only test for ports ('crosstalking')
                        .mulitcastAdr("229.9.9.13")  // ip4 multicast address
                        .setDgramsize(16_000)         // datagram size. Small sizes => lower latency, large sizes => better throughput [range 1200 to 64_000 bytes]
                        .socketReceiveBufferSize(4_000_000) // as large as possible .. however avoid hitting system limits in example
                        .socketSendBufferSize(2_000_000)
                        // uncomment this to enable spin looping. Will increase throughput once datagram size is lowered below 8kb or so
//                        .idleParkMicros(1)
//                        .spinLoopMicros(100_000)
        );

    }

}
