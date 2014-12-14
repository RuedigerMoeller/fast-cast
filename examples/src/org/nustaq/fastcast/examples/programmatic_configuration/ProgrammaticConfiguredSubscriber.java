package org.nustaq.fastcast.examples.programmatic_configuration;

import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.convenience.ObjectSubscriber;

/**
 * Created by ruedi on 14.12.14.
 */
public class ProgrammaticConfiguredSubscriber {

    public static void main( String arg[] ) {
        FastCast.getFastCast().setNodeId("SUBS"); // 5 chars MAX !!
        ProgrammaticConfiguredPublisher.configureFastCast();
        FastCast.getFastCast().onTransport("default").subscribe(
            new SubscriberConf(1) // listen to topic 1
                .receiveBufferPackets(20000), // how many packets to buffer in case of a loss+retransmission
            new ObjectSubscriber() {
                long lastMsg = System.currentTimeMillis();
                int msgReceived = 0;

                @Override
                protected void objectReceived(String sender, long sequence, Object msg) {
                    msgReceived++;
                    if ( System.currentTimeMillis()-lastMsg > 1000 ) {
                        System.out.println("received from "+sender+" number of msg "+msgReceived);
                        System.out.println("current: "+msg);
                        lastMsg = System.currentTimeMillis();
                        msgReceived = 0;
                    }
                }

                @Override
                public boolean dropped() {
                    System.out.println("Fatal: could not keep up with send rate. exiting");
                    System.exit(0);
                    return false; // do not attempt resync
                }
            }
       );
    }
}
