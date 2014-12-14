/*
 * Copyright 2014 Ruediger Moeller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nustaq.fastcast.convenience;

import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

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
    protected boolean decodeInReceiver = false;

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

    /**
     * default is false. If true, deserialization is done directly inside the messaging thread
     * saving a tmp byte array alloc for each incoming message. Downside is possible slowdown / packet loss
     * in case of large messages coming in.
     *
     * @param b
     * @return
     */
    public ObjectSubscriber decodeInReceiverThread(boolean b) {
        decodeInReceiver = b;
        return this;
    }

    @Override
    public void messageReceived(final String sender, final long sequence, Bytez b, long off, int len) {
        if ( ! decodeInReceiver && executor != null )
            super.messageReceived(sender, sequence, b, off, len);
        else {
            // directly decode. saves tmp byte array alloc for each message
            if ( tmpBuf.length < len ) {
                tmpBuf = new byte[len];
            }
            b.getArr(off, tmpBuf, 0, len); // fixme: could be zerocopy using OffHeapCoder
            final Object objectMessage = coder.toObject(tmpBuf, 0, tmpBuf.length);
            if ( executor != null ) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        objectReceived(sender, sequence, objectMessage);
                    }
                });
            } else {
                objectReceived(sender, sequence, objectMessage);
            }
        }
    }

    /**
     * note: this is not called in case decodeInReceiver is true or no dedicated thread is pressent.
     * So take care when overriding
     */
    @Override
    public void messageReceived(final String sender, final long sequence, byte bytes[], int off, int len) {
        Object msg = coder.toObject(bytes, off, len);
        objectReceived(sender,sequence,msg);
    }

    protected abstract void objectReceived(String sender, long sequence, Object msg);

}
