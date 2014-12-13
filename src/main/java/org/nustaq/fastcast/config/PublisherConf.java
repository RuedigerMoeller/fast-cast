package org.nustaq.fastcast.config;

/**
 * Created by ruedi on 29.11.2014.
 */
public class PublisherConf {

    int topicId;

    // Buffer sizes are very important. For high volume senders, the send buffer must be large
    // these defaults hold for moderate traffic

    ///////////////////////////////////////////////////////////////////////////////
    //
    // buffers
    //
    ///////////////////////////////////////////////////////////////////////////////

    // overall send history
    int numPacketHistory = 500_000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // timings
    //
    ///////////////////////////////////////////////////////////////////////////////

    long heartbeatInterval = 200;    // sent per topicId, ms. detects senderTimeoutMillis

    int pps = 10_000_000; // rate limit datagram per second
    int ppsWindow = 100;   // time window rate limit is checked. e.g. ppsWindow = 10 => 1 sec/10 = 100ms another: ppsWindow = 1000 => 1 ms

    public PublisherConf() {
    }

    public PublisherConf(int topicId) {
        this.topicId = topicId;
    }

    public int getPpsWindow() {
        return ppsWindow;
    }

    public PublisherConf ppsWindow(int ppsWindow) {
        this.ppsWindow = ppsWindow;
        return this;
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // sender misc (threading, locking
    //
    ///////////////////////////////////////////////////////////////////////////////

    public int getNumPacketHistory() {
        return numPacketHistory;
    }

    public PublisherConf numPacketHistory(int numPacketHistory) {
        this.numPacketHistory = numPacketHistory;
        return this;
    }

    public int getTopicId() {
        return topicId;
    }

    public PublisherConf topicId(int topicId) {
        this.topicId = topicId;
        return this;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public PublisherConf heartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public int getPps() {
        return pps;
    }

    public PublisherConf pps(int pps) {
        this.pps = pps;
        return this;
    }
}
