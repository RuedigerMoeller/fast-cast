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

    long heartbeatInterval = 500;    // sent per topicId, ms. detects senderTimeoutMillis

    int pps = 10_000_000; // rate limit datagram per second
    // unused, deprecated
    int ppsWindow = 1000;   // time window rate limit is checked. e.g. ppsWindow = 10 => 1 sec/10 = 100ms another: ppsWindow = 1000 => 1 ms

    public PublisherConf() {
    }

    public PublisherConf(int topicId) {
        this.topicId = topicId;
    }

    @Deprecated
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
