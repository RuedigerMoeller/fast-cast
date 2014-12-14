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
    int receiveBufferPackets = 5000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // timings
    //
    ///////////////////////////////////////////////////////////////////////////////

    // time interval until a receiver sends a retransmission request after a gap
    long maxDelayRetransMS = 0;
    // time until a retransrequest is sent again if sender does not fulfill
    long maxDelayNextRetransMS = 20;

    // time until a sender is lost+deallocated if it stops sending heartbeats
    // on overload i had crashes from false overload induced timeouts and receive of packets after buffer dealloc
    // pls report once you observe crashes !
    long senderHBTimeout = 30000;

    ///////////////////////////////////////////////////////////////////////////////
    //
    // receiver misc
    //
    ///////////////////////////////////////////////////////////////////////////////

    // accept packet loss. don't send retransmissions. Note that this only works for messages < datagramsize (see transport conf)
    boolean unreliable = false;

    public SubscriberConf() {
    }

    public SubscriberConf(int topicId) {
        this.topicId = topicId;
    }

    public int getTopicId() {
        return topicId;
    }

    public SubscriberConf topicId(int topicId) {
        this.topicId = topicId;
        return this;
    }

    public int getReceiveBufferPackets() {
        return receiveBufferPackets;
    }

    public SubscriberConf receiveBufferPackets(int receiveBufferPackets) {
        this.receiveBufferPackets = receiveBufferPackets;
        return this;
    }

    public long getMaxDelayRetransMS() {
        return maxDelayRetransMS;
    }

    public SubscriberConf maxDelayRetransMS(long maxDelyRetransMS) {
        this.maxDelayRetransMS = maxDelyRetransMS;
        return this;
    }

    public long getMaxDelayNextRetransMS() {
        return maxDelayNextRetransMS;
    }

    public SubscriberConf maxDelayNextRetransMS(long maxDelayNextRetransMS) {
        this.maxDelayNextRetransMS = maxDelayNextRetransMS;
        return this;
    }

    public long getSenderHBTimeout() {
        return senderHBTimeout;
    }

    public boolean isUnreliable() {
        return unreliable;
    }

    public SubscriberConf unreliable(boolean unreliable) {
        this.unreliable = unreliable;
        return this;
    }
}
