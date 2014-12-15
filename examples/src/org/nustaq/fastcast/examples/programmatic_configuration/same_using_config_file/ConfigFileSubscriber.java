package org.nustaq.fastcast.examples.programmatic_configuration.same_using_config_file;

import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.convenience.ObjectSubscriber;
import org.nustaq.fastcast.examples.programmatic_configuration.ProgrammaticConfiguredPublisher;

/**
 * Created by ruedi on 15.12.14.
 */
public class ConfigFileSubscriber {

    public static void main( String arg[] ) throws Exception {
        FastCast.getFastCast().setNodeId("csub"); // 5 chars MAX !!
        FastCast.getFastCast()
            .loadConfig(ConfigFilePublisher.CFG_FILE)
            .onTransport("default").subscribe("oneAndOnlyTopic",
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
