package org.nustaq.fastcast.config;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/5/13
 * Time: 5:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubscriberConf {

    int topicId;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // buffers
    //
    ///////////////////////////////////////////////////////////////////////////////

    // in case of gaps, buffer that many received packets (=datagram in fastcast context).
    // KEPT PER SENDER PER TOPIC !. So value of 10 with 10 senders on 2 topics = 20000 = 160MB with 8kb packets
    // increase for high volume receivers causing retransmissions (the larger, the fewer retransmissions will be there)
    int receiveBufferPackets = 100_000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // timings
    //
    ///////////////////////////////////////////////////////////////////////////////

    // time interval until a receiver sends a retransmission request after a gap
    long maxDelayRetransMS = 1;
    // time until a retransrequest is sent again if sender does not fulfill
    long maxDelayNextRetransMS = 5;

    // time until a sender is lost+deallocated if it stops sending heartbeats
    long senderHBTimeout = 5000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // receiver misc (threading, locking
    //
    ///////////////////////////////////////////////////////////////////////////////

    public SubscriberConf() {
    }

    public SubscriberConf(int topicId) {
        this.topicId = topicId;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public int getReceiveBufferPackets() {
        return receiveBufferPackets;
    }

    public void setReceiveBufferPackets(int receiveBufferPackets) {
        this.receiveBufferPackets = receiveBufferPackets;
    }

    public long getMaxDelayRetransMS() {
        return maxDelayRetransMS;
    }

    public void setMaxDelayRetransMS(long maxDelyRetransMS) {
        this.maxDelayRetransMS = maxDelyRetransMS;
    }

    public long getMaxDelayNextRetransMS() {
        return maxDelayNextRetransMS;
    }

    public void setMaxDelayNextRetransMS(long maxDelayNextRetransMS) {
        this.maxDelayNextRetransMS = maxDelayNextRetransMS;
    }

    public long getSenderHBTimeout() {
        return senderHBTimeout;
    }
}
