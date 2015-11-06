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
    long maxDelayRetransMS = 1;
    // time until a retransrequest is sent again if sender does not fulfill
    long maxDelayNextRetransMS = 5;

    // time until a sender is lost+deallocated if it stops sending heartbeats
    // on overload i had crashes from false overload induced timeouts and receive of packets after buffer dealloc
    // pls report once you observe crashes !
    long senderHBTimeout = 10000;

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
    
    public SubscriberConf setSenderHBTimeout() {
        this.senderHBTimeout = senderHBTimeout;
        return this;
    }

    public boolean isUnreliable() {
        return unreliable;
    }

    public SubscriberConf unreliable(boolean unreliable) {
        this.unreliable = unreliable;
        return this;
    }
}
