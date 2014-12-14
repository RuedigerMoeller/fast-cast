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
