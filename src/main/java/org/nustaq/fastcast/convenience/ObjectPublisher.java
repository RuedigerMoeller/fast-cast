package org.nustaq.fastcast.convenience;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

/**
 * Created by ruedi on 13.12.14.
 */
public class ObjectPublisher {

    protected FCPublisher pub;
    protected FSTCoder coder;

    public ObjectPublisher(FCPublisher pub) {
        this.pub = pub;
        coder = new DefaultCoder();
    }

    public ObjectPublisher(FCPublisher pub, Class ... preregister) {
        this.pub = pub;
        coder = new DefaultCoder(preregister);
    }

    public void sendObject( String receiverNodeIdOrNull, Object toSend, boolean flush ) {
        byte[] bytes = coder.toByteArray(toSend);// fixme: performance. Need zerocopy variant in DefaultCoder
        while( ! pub.offer(receiverNodeIdOrNull,bytes,0,bytes.length,flush) ) {
            // spin
        }
    }

    public FCPublisher getPub() {
        return pub;
    }

    public ObjectPublisher batchOnLimit(boolean doBatch) {
        pub.batchOnLimit(doBatch);
        return this;
    }


}
