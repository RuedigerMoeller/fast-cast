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
package org.nustaq.fastcast.api;

import org.nustaq.offheap.bytez.Bytez;

/**
 * Created by ruedi on 29.11.2014.
 */
public interface FCSubscriber {

    public void messageReceived(String sender, long sequence, Bytez b, long off, int len);

    /**
     * called in case the receiver was too slow in processing messages
     * and therefore got dropped (unrecoverable message loss).
     * return true in order to let fast-cast automatically reconnect and resync
     */
    public boolean dropped();

    /**
     * a sender stopped sending or terminated
     * @param senderNodeId
     */
    public void senderTerminated( String senderNodeId );

    /**
     * called upon the first message regulary received from a sender
     * @param receivesFrom
     * @param seqNo
     */
    public void senderBootstrapped(String receivesFrom, long seqNo);

//    public void resync(); FIXME
}
