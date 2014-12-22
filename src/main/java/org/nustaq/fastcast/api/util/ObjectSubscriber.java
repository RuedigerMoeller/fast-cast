/**
 * Copyright (c) 2014, Ruediger Moeller. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 *
 * Date: 03.01.14
 * Time: 21:19
 * To change this template use File | Settings | File Templates.
 */
package org.nustaq.fastcast.api.util;

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
