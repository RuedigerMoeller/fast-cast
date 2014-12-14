package org.nustaq.fastcast.convenience;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.serialization.simpleapi.DefaultCoder;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 14.12.2014.
 *
 * subscriber implementation that handles conversion to byte array adn by default provides a dedicated thread for
 * message processing. Note this is not allocation free.
 */
public abstract class ByteArraySubscriber implements FCSubscriber {

    protected Executor executor;

    public ByteArraySubscriber( boolean dedicatedThread) {
        if ( dedicatedThread ) {
            executor = Executors.newSingleThreadExecutor();
        }
    }

    public ByteArraySubscriber() {
        this(true);
    }

    @Override
    public void messageReceived(final String sender, final long sequence, Bytez b, long off, final int len) {
        final byte[] bytes = b.toBytes(off, len); // fixme: copy is needed only in case of dedicated thread
        if ( executor != null ) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    messageReceived(sender,sequence,bytes);
                }
            });
        } else {
            messageReceived(sender,sequence,bytes);
        }
    }

    protected abstract void messageReceived(String sender, long sequence, byte[] msg);

    @Override
    public boolean dropped() {
        return true; // resync
    }

    @Override
    public void senderTerminated(String senderNodeId) {
        // do nothing
    }

    @Override
    public void senderBootstrapped(String receivesFrom, long seqNo) {
        // do nothing
    }

}
