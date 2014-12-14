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
