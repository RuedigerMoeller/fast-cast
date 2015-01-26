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
package org.nustaq.fastcast.api;

import org.nustaq.offheap.bytez.Bytez;

/**
 * interface to be implemented by a subscriber to a topic. Note there is a serialization based util implementation ObjectSubscriber.
 *
 * Created by ruedi on 29.11.2014.
 */
public interface FCSubscriber {

    /**
     * a message has been defragmented and received. This method is called directly in the packet receiving
     * thread, so processing should be delegated to a worker or be extremely quick. else packet loss might occur.
     *
     * one can use 'Bytez.getArr(off, tmpBuf, 0, len);' to copy the message contents to a byte array 'tmpBuf'.
     *
     * @param sender - nodeId of sending process
     * @param sequence - internal sequence number (of interest for unreliable variants)
     * @param b - bytesource containing the message at off and with len
     * @param off - offset of message
     * @param len - len of message
     */
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
