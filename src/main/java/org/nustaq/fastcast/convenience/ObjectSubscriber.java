package org.nustaq.fastcast.convenience;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.onheap.HeapBytez;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 13.12.14.
 *
 * A subscriber implementation receiving and automatically decoding serialized objects.
 * By default it uses a dedicated thread for delivery to avoid blocking the receiverThread
 * pulling data packets from the network.
 * Decoding is done 'inside' the messaging callback thread, though.
 *
 */
public abstract class ObjectSubscriber implements FCSubscriber {

    protected FSTCoder coder;
    Executor executor;

    public ObjectSubscriber( boolean dedicatedThread, Class ... preregister) {
        coder = new DefaultCoder(true,preregister);
        if ( dedicatedThread ) {
            executor = Executors.newSingleThreadExecutor();
        }
    }

    public ObjectSubscriber( Class ... preregister) {
        this(true,preregister);
    }

    public ObjectSubscriber(boolean dedicatedThread) {
        this(dedicatedThread,new Class[0]);
    }

    public ObjectSubscriber() {
        this(true);
    }

    @Override
    public void messageReceived(final String sender, final long sequence, Bytez b, long off, final int len) {
        final byte[] bytes = b.toBytes(off, len);
        if ( executor != null ) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Object msg = coder.toObject(bytes, 0, len);
                    objectReceived(sender,sequence,msg);
                }
            });
        } else {
            objectReceived(sender,sequence,coder.toObject(bytes, 0, len));
        }
    }

    protected abstract void objectReceived(String sender, long sequence, Object msg);

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
