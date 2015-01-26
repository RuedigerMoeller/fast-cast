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
 * interface used to send messages. obtained like 'fastCast.onTransport("default").publish("stream");'
 *
 * Created by ruedi on 29.11.2014.
 */
public interface FCPublisher {

    /**
     * send a byte[] message
     *
     * @param receiverNodeId - null to send to all subscribers, aNodeId to target a specific node
     * @param b - bytes to send
     * @param start - start offset
     * @param len - number of bytes to send
     * @param doFlush - if true, the currently written packet is sent immediately EXCEPT your send rate is > pps limit (packets per second)
     *                only set to true if you care mostly about latency instead of throughput. fast cast automatically flushes after one ms. So a
     *                value of true only makes sense if you care about micros.
     * @return true if the message has been sent, false if limits/buffers are full.
     */
    public boolean offer(String receiverNodeId, byte b[], int start, int len, boolean doFlush);
    /**
     * send a byte[] message
     *
     * @param receiverNodeId - null to send to all subscribers, aNodeId to target a specific node
     * @param msg - byte source containing the message bytes
     * @param start - start offset
     * @param len - number of bytes to send
     * @param doFlush - if true, the currently written packet is sent immediately EXCEPT your send rate is > pps limit (packets per second)
     *                only set to true if you care mostly about latency instead of throughput. fast cast automatically flushes after one ms. So a
     *                value of true only makes sense if you care about micros.
     * @return true if the message has been sent, false if limits/buffers are full.
     */
    public boolean offer(String receiverNodeId, ByteSource msg, long start, int len, boolean doFlush);

    /**
     * send a byte[] message
     *
     * @param receiverNodeId - null to send to all subscribers, aNodeId to target a specific node
     * @param msg - byte source containing the message bytes.
     * @param doFlush - if true, the currently written packet is sent immediately EXCEPT your send rate is > pps limit (packets per second)
     *                only set to true if you care mostly about latency instead of throughput. fast cast automatically flushes after one ms. So a
     *                value of true only makes sense if you care about micros.
     * @return true if the message has been sent, false if limits/buffers are full.
     */
    public boolean offer(String receiverNodeId, ByteSource msg, boolean doFlush);

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
