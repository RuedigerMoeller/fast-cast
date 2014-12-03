package org.nustaq.fastcast.config;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/5/13
 * Time: 5:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCSubscriberConf {

    String transport = "default";
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
    // receiver timings
    //
    ///////////////////////////////////////////////////////////////////////////////

    long maxDelayRetransMS = 1; // send retrans
    long maxDelayNextRetransMS = 5;
    private long senderHBTimeout = 3000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // receiver misc (threading, locking
    //
    ///////////////////////////////////////////////////////////////////////////////

    public FCSubscriberConf() {
    }

    public FCSubscriberConf(String transport, int topicId) {
        this.transport = transport;
        this.topicId = topicId;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
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
