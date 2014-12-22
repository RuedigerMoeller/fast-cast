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

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.offheap.bytez.Bytez;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 14.12.2014.
 *
 * subscriber implementation that handles conversion to byte array and by default provides a
 * dedicated thread for message processing. Note this is *not* allocation free, so not well suited for
 * low latency stuff.
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

    protected byte tmpBuf[] = new byte[0];
    @Override
    public void messageReceived(final String sender, final long sequence, Bytez b, long off, final int len) {
        if ( executor != null ) {
            final byte[] bytes = b.toBytes(off, len);
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    messageReceived(sender,sequence,bytes, 0, len);
                }
            });
        } else {
            // directly decode. saves tmp byte array alloc for each message
            if ( tmpBuf.length < len ) {
                tmpBuf = new byte[len];
            }
            b.getArr(off, tmpBuf, 0, len);
            messageReceived(sender,sequence,tmpBuf,0,len);
        }
    }

    protected abstract void messageReceived(String sender, long sequence, byte[] msg, int off, int len);

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
