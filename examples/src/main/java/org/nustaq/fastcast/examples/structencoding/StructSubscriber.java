package org.nustaq.fastcast.examples.structencoding;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;
import static org.nustaq.fastcast.examples.structencoding.Protocol.*;

/**
 * Created by moelrue on 12/15/14.
 */
public class StructSubscriber {

    public static void main( String arg[] ) {
        Protocol.initStructFactory();

        FastCast.getFastCast().setNodeId("SUB"); // 5 chars MAX !!
        StructPublisher.configureFastCast();
        final RateMeasure rateMeasure = new RateMeasure("receive rate");

        FastCast.getFastCast().onTransport("default").subscribe(
            new SubscriberConf(1).receiveBufferPackets(33_000),
            new FCSubscriber() {

                PriceUpdateStruct msg = FSTStructFactory.getInstance().createEmptyStructPointer(PriceUpdateStruct.class);

                @Override
                public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                    msg.baseOn(b, (int) off);
                    rateMeasure.count();
                    // instanceof'ing in case of various messages
                    // Class msgStruct = msg.getPointedClass(); otherStructClass = msg.detachTo(otherStructClass);
                }

                @Override
                public boolean dropped() {
                    System.out.println("fatal, could not keep up. exiting");
                    System.exit(0);
                    return false;
                }

                @Override
                public void senderTerminated(String senderNodeId) {
                    System.out.println("sender died "+senderNodeId);
                }

                @Override
                public void senderBootstrapped(String receivesFrom, long seqNo) {
                    System.out.println("bootstrap "+receivesFrom);
                }
            }
        );
    }
}
