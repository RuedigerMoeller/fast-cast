package org.nustaq.fastcast.examples.structencoding;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.convenience.ObjectPublisher;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.structs.FSTStructAllocator;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by moelrue on 12/15/14.
 */
public class StructPublisher {

    public static void main(String arg[]) {

        FastCast.getFastCast().setNodeId("PUB"); // 5 chars MAX !!
        configureFastCast();

        FCPublisher pub = FastCast.getFastCast().onTransport("default").publish(
                new PublisherConf(1)            // unique-per-transport topic id
                    .numPacketHistory(33_000)   // how long packets are kept for retransmission requests
                            // beware: memory usage (kept offheap) = dgramsize * history = 2500*40000 = 100MB (GC safe for 8 seconds)
                    .pps(15_000)                  // packets per second rate limit. So max traffic for topic = 5000*2500 = 12.5 MB/second
        );

        Protocol.initStructFactory();

        Protocol.PriceUpdateStruct template = new Protocol.PriceUpdateStruct();
        FSTStructAllocator onHeapAlloc = new FSTStructAllocator(0);

        Protocol.PriceUpdateStruct msg = onHeapAlloc.newStruct(template); // speed up instantiation

        ThreadLocalRandom current = ThreadLocalRandom.current();
        // could directly send raw on publisher
        RateMeasure measure = new RateMeasure("msg/s");
        while( true ) {
            measure.count();

            // fill in data
            Protocol.InstrumentStruct instrument = msg.getInstrument();
            instrument.getMnemonic().setString("BMW");
            instrument.setInstrumentId(13);
            msg.setPrc(99.0+current.nextDouble(10.0)-5);
            msg.setQty(100+current.nextInt(10));

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
                        .port(42043)                // port is more important than address as some OS only test for ports ('crosstalking')
                        .mulitcastAdr("229.9.9.9")  // ip4 multicast address
                        .setDgramsize(64_000)         // datagram size. Small sizes => lower latency, large sizes => better throughput
                        .socketReceiveBufferSize(4_000_000) // as large as possible .. however avoid hitting system limits in example
                        .socketSendBufferSize(2_000_000)
//                        .idleParkMicros(1)
//                        .spinLoopMicros(100_000)
        );

    }
}
