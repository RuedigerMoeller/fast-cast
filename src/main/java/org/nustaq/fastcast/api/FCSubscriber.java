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
