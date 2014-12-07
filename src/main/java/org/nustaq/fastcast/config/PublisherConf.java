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

    public void setPpsWindow(int ppsWindow) {
        this.ppsWindow = ppsWindow;
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // sender misc (threading, locking
    //
    ///////////////////////////////////////////////////////////////////////////////

    public int getNumPacketHistory() {
        return numPacketHistory;
    }

    public void setNumPacketHistory(int numPacketHistory) {
        this.numPacketHistory = numPacketHistory;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public int getPps() {
        return pps;
    }

    public void setPps(int pps) {
        this.pps = pps;
    }
}
