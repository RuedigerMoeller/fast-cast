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
