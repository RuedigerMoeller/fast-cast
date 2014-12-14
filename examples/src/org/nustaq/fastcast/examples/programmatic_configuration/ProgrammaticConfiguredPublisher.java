package org.nustaq.fastcast.examples.programmatic_configuration;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.convenience.ObjectPublisher;
import org.nustaq.fastcast.util.RateMeasure;

/**
 * Created by ruedi on 14.12.14.
 */
public class ProgrammaticConfiguredPublisher {

    public static void main(String arg[]) {

        FastCast.getFastCast().setNodeId("PUB"); // 5 chars MAX !!
        configureFastCast();

        FCPublisher pub = FastCast.getFastCast().onTransport("default").publish(
            new PublisherConf(1)            // unique-per-transport topic id
                .numPacketHistory(40_000)   // how long packets are kept for retransmission requests
                                            // beware: memory usage (kept offheap) = dgramsize * history = 2500*40000 = 100MB (GC safe for 8 seconds)
                .pps(5000)                  // packets per second rate limit. So max traffic for topic = 5000*2500 = 12.5 MB/second
        );

        // could directly send raw on publisher
        // while( ! pub.offer(..) ) { /* spin */ }

        // or use a helper for fast-serialized messages
        ObjectPublisher opub = new ObjectPublisher(pub);
        RateMeasure measure = new RateMeasure("msg/s");
        while( true ) {
            measure.count();
            opub.sendObject(
                null,  // all listeners should receive (by specifying a nodeId, a specific subscriber can be targeted)
                "Hello "+System.currentTimeMillis(), // serializable object
                false  // allow for 'batching' several messages into one (will create slight latency)
            );
        }
    }

    public static void configureFastCast() {
        // note this configuration is far below possible limits regarding throughput and rate
        FastCast fc = FastCast.getFastCast();
        fc.addTransport(
            new PhysicalTransportConf("default")
                .interfaceAdr("127.0.0.1")  // define the interface
                .port(42042)                // port is more important than address as some OS only test for ports ('crosstalking')
                .mulitcastAdr("229.9.9.9")  // ip4 multicast address
                .setDgramsize(2500)         // datagram size. Small sizes => lower latency, large sizes => better throughput
                .socketReceiveBufferSize(4_000_000) // as large as possible .. however avoid hitting system limits in example
                .socketSendBufferSize(2_000_000)
        );

    }
}
