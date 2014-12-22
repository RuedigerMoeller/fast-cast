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

import org.nustaq.offheap.bytez.ByteSource;

/**
 * Created by ruedi on 29.11.2014.
 */
public interface FCPublisher {

    /**
     *
     * @param receiverNodeId - null for all subscribers of topic, a nodeId for a specific subscriber (unicast)
     * @param msg
     * @param start
     * @param len
     * @param doFlush
     * @return
     */
    public boolean offer(String receiverNodeId, byte b[], int start, int len, boolean doFlush);
    public boolean offer(String receiverNodeId, ByteSource msg, long start, int len, boolean doFlush);
    public boolean offer(String subscriberNodeId, ByteSource msg, boolean doFlush);
    public int getTopicId();
    public void flush();

    // FIXME: add listener interface
//    public void retransMissionRequestReceived(int percentageOfHistory, String nodeId);

    public void setPacketRateLimit(int limit);
    public int getPacketRateLimit();

    /**
     * decides wether to start batching once rate limit is reached. Default is true.
     * Note this must be set to false in case there are unreliable or unordered receivers amongst
     * subscribers.
     * Batching means to pack several small messages into one datagram packet.
     *
     * @param doBatch
     * @return
     */
    public FCPublisher batchOnLimit(boolean doBatch);
    public boolean isBatchOnLimit(boolean doBatch);

}
