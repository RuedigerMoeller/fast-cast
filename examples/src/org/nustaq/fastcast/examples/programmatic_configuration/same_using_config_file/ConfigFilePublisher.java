package org.nustaq.fastcast.examples.programmatic_configuration.same_using_config_file;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.util.ObjectPublisher;
import org.nustaq.fastcast.util.RateMeasure;

/**
 * Created by ruedi on 15.12.14.
 */
public class ConfigFilePublisher {

    public static final String CFG_FILE = "examples/src/org/nustaq/fastcast/examples/programmatic_configuration/same_using_config_file/config.kson";

    public static void main( String arg[] ) throws Exception {
        FastCast.getFastCast().setNodeId("CPUB"); // max 5 chars !
        // note this configuration is far below possible limits regarding throughput and rate
        FastCast fc = FastCast.getFastCast().loadConfig(CFG_FILE);

        FCPublisher pub = FastCast.getFastCast().onTransport("default").publish("oneAndOnlyTopic");

        // use a helper for fast-serialized messages
        ObjectPublisher opub = new ObjectPublisher(pub);
        RateMeasure measure = new RateMeasure("msg/s");
        while( true ) {
            measure.count();
            opub.sendObject(
                null,  // all listeners should receive this (by specifying a nodeId, a specific subscriber can be targeted)
                "Hello "+System.currentTimeMillis(), // serializable object
                false  // allow for 'batching' several messages into one (will create slight latency)
            );
        }

    }

}
