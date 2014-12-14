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
 *
 */
public abstract class ObjectSubscriber extends ByteArraySubscriber {

    protected FSTCoder coder;

    public ObjectSubscriber( boolean dedicatedThread, Class ... preregister) {
        super(dedicatedThread);
        coder = new DefaultCoder(true,preregister);
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
    public void messageReceived(final String sender, final long sequence, byte bytes[]) {
        Object msg = coder.toObject(bytes, 0, bytes.length);
        objectReceived(sender,sequence,msg);
    }

    protected abstract void objectReceived(String sender, long sequence, Object msg);

}
