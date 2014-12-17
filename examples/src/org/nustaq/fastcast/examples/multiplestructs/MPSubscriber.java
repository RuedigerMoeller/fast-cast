package org.nustaq.fastcast.examples.multiplestructs;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.examples.multiplestructs.MultipleProtocol.*;
import org.nustaq.fastcast.examples.structencoding.StructPublisher;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

/**
 * Created by ruedi on 17.12.14.
 */
public class MPSubscriber {

    public static void main( String arg[] ) {
        MultipleProtocol.initStructFactory();

        FastCast.getFastCast().setNodeId("MSUB"); // 5 chars MAX !!
        MPPublisher.configureFastCast();
        final RateMeasure rateMeasure = new RateMeasure("receive rate");

        FastCast.getFastCast().onTransport("default").subscribe(
            new SubscriberConf(1).receiveBufferPackets(33_000),
            new FCSubscriber() {

                FSTStruct msg = FSTStructFactory.getInstance().createEmptyStructPointer(FSTStruct.class);
                int count = 0;

                @Override
                public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                    rateMeasure.count();

                    msg.baseOn(b, (int) off);

                    Class type = msg.getPointedClass();
                    if ( type == AMessage.class ) {
                        AMessage am = msg.cast();
                        // am is valid until another pointer is used. use "am = am.detach" in order to get a non-cached pointer
                    } else if ( type == OtherMessage.class ) {
                        OtherMessage om = msg.cast();
                        // ..
                    } else if ( type == ComposedMessage.class ) {
                        ComposedMessage cm = msg.cast();
                        if ( count++ == 500_000 ) {
                            System.out.println("Other"+cm);
                            count = 0;
                        }
                    }

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
