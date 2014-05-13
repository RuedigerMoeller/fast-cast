package de.ruedigermoeller.fastcast.config;

import de.ruedigermoeller.fastcast.util.FCLog;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/5/13
 * Time: 5:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCTopicConf {

    // fixme: find a better place
    public static long heartbeatInterval = 200;    // sent per topic, ms. detects senderTimeoutMillis
    public static long flowControlInterval = 1000; // time window(ms) flow control uses to determine send rate + stats reset rate

    String name;
    String transport = "default";
    int topic;
    String serviceClass;

    // request response: maximum unanswered calls before blocking
    int maxOpenRespondedCalls = 10000;
    // request response: when a call timeout is signaled on FCFutureResult
    int responseMethodsTimeout = 3000;

    // Buffer sizes are very important. For high volume senders, the send buffer must be large
    // these defaults hold for moderate traffic

    ///////////////////////////////////////////////////////////////////////////////
    //
    // buffers
    //
    ///////////////////////////////////////////////////////////////////////////////

    // overall send history on heap
    int numPacketHistory = 5000;
    // part of on heap history used for send queue (after that send might start blocking). Beware, increasing this will increase latency
    int maxSendPacketQueueSize = 100;

    // in case of gaps, buffer that many received packets (=datagram in fastcast context).
    // KEPT PER SENDER PER TOPIC !. So value of 10 with 10 senders on 2 topics = 20000 = 160MB with 8kb packets
    // increase for high volume receivers causing retransmissions (the larger, the fewer retransmissions will be there)
    int receiveBufferPackets = 1000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // timings
    //
    ///////////////////////////////////////////////////////////////////////////////

    // defines at which rate this topic send messages.
    // This is the major rate limiting throttle influencing throughput and overload control
    // if too low => lots of retransmission
    // if too high => no throughput
    int sendPauseMicros = -1;

    // when a sender does not send on a topic for this time, drop it and free memory.
    // if the sender starts sending again, a resync will be done as if the sender has
    // joined the cluster as a new node
    long senderTimeoutMillis = 10000;

    long maxDelayRetransMS = 1; // send retrans
    long maxDelayNextRetransMS = 5;


    ///////////////////////////////////////////////////////////////////////////////
    //
    // misc (threading, locking
    //
    ///////////////////////////////////////////////////////////////////////////////

    boolean perSenderThread = false; //
    boolean decodeInTransportThread = false; // no receiver thread+queue at all
    boolean optForLatency = false; // do not chain msg across packat if not necessary
    boolean useSpinlockInSendQueue = false; // may create sender side latency in favor of saving cpu
    String flowControlClass = null;//StupidFlowControl.class.getName();  // set null to delegate flow control to service
    int decodeQSize = 10000; // max size of undecoded messages to buffer in case decoding+execution is too slow
    boolean autoStart = false;
    private int dGramRate; // ignored if 0

    public FCTopicConf() {
    }

    public FCTopicConf(String name, String transport, int topic, String serviceClass) {
        this.name = name;
        this.transport = transport;
        this.topic = topic;
        this.serviceClass = serviceClass;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public String getFlowControlClass() {
        return flowControlClass;
    }

    public void setFlowControlClass(String flowControlClass) {
        this.flowControlClass = flowControlClass;
    }

    public boolean isPerSenderThread() {
        return perSenderThread;
    }

    public void setPerSenderThread(boolean perSenderThread) {
        this.perSenderThread = perSenderThread;
    }

    public boolean isOptForLatency() {
        return optForLatency;
    }

    public void setOptForLatency(boolean optForLatency) {
        this.optForLatency = optForLatency;
    }

    public boolean isDecodeInTransportThread() {
        return decodeInTransportThread;
    }

    public void setDecodeInTransportThread(boolean decodeInTransportThread) {
        this.decodeInTransportThread = decodeInTransportThread;
    }

    public int getNumPacketHistory() {
        return numPacketHistory;
    }

    public void setNumPacketHistory(int numPacketHistory) {
        this.numPacketHistory = numPacketHistory;
    }

    public FCTopicConf(String name) {
        this.name = name;
    }

    public int getResponseMethodsTimeout() {
        return responseMethodsTimeout;
    }

    public void setResponseMethodsTimeout(int responseMethodsTimeout) {
        this.responseMethodsTimeout = responseMethodsTimeout;
    }

    public int getMaxOpenRespondedCalls() {
        return maxOpenRespondedCalls;
    }

    public void setMaxOpenRespondedCalls(int maxOpenRespondedCalls) {
        this.maxOpenRespondedCalls = maxOpenRespondedCalls;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public int getTopic() {
        return topic;
    }

    public void setTopic(int topic) {
        this.topic = topic;
    }

    public String getServiceClass() {
        return serviceClass;
    }

    public void setServiceClass(String serviceClass) {
        this.serviceClass = serviceClass;
    }

    public int getReceiveBufferPackets() {
        return receiveBufferPackets;
    }

    public void setReceiveBufferPackets(int receiveBufferPackets) {
        this.receiveBufferPackets = receiveBufferPackets;
    }

    public int getSendPauseMicros() {
        return sendPauseMicros == -1 ? 300:sendPauseMicros;
    }

//    public void setSendPauseMicros(int sendPauseMicros) {
//        if ( sendPauseMicros != -1 ) {
//            FCLog.get().warn("both sendpause and DGramRate configured. Expect only one of them to be set.");
//        }
//        this.sendPauseMicros = sendPauseMicros;
//    }

    public int getMaxSendPacketQueueSize() {
        return maxSendPacketQueueSize;
    }

    public void setMaxSendPacketQueueSize(int maxSendPacketQueueSize) {
        this.maxSendPacketQueueSize = maxSendPacketQueueSize;
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

    public boolean useSpinlockInSendQueue() {
        return useSpinlockInSendQueue;
    }

    public void setUseSpinlockInSendQueue(boolean useSpinlockInSendQueue) {
        this.useSpinlockInSendQueue = useSpinlockInSendQueue;
    }

    public int getDecodeQSize() {
        return decodeQSize;
    }

    public void setDecodeQSize(int decodeQSize) {
        this.decodeQSize = decodeQSize;
    }

    public long getSenderTimeoutMillis() {
        return senderTimeoutMillis;
    }

    public void setSenderTimeoutMillis(long senderTimeoutMillis) {
        this.senderTimeoutMillis = senderTimeoutMillis;
    }

    /**
     * warning: this overwrite sendPauseMicros
     * @param DGramRate
     */
    public void setDGramRate(int DGramRate) {
        if ( DGramRate == 0 )
            return;
        int slowdown = 1000*1000/DGramRate;
        if ( slowdown < 1 ) {
            slowdown = 1;
        }
        sendPauseMicros = slowdown;
        this.dGramRate = DGramRate;
    }

    public int getDGramRate() {
        return dGramRate;
    }

}
