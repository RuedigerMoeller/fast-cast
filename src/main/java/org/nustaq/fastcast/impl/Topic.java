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
package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.transport.PhysicalTransport;
import java.util.List;

/*
* Created with IntelliJ IDEA.
* User: ruedi
* Date: 23.08.13
* Time: 02:04
* To change this template use File | Settings | File Templates.
*/

/**
 * Combines publisher+subscriber configuration, topic stats ..
 */
public class Topic {

    PublisherConf publisherConf;
    SubscriberConf subscriberConf;

    TransportDriver channelDispatcher;
    PacketSendBuffer sender;

    private FCSubscriber subscriber;
    int topicId = -1;
    private long hbTimeoutMS = 3000;

    public Topic(SubscriberConf subscriberConf, PublisherConf publisherConf) {
        this.subscriberConf = subscriberConf;
        this.publisherConf = publisherConf;
        if ( subscriberConf != null ) {
            hbTimeoutMS = subscriberConf.getSenderHBTimeout();
        }
    }

    public List<String> getTimedOutSenders(List<String> res, long now, long timeout) {
        ReceiveBufferDispatcher receiver = channelDispatcher.getReceiver(topicId);
        if ( receiver != null ) {
            receiver.getTimedOutSenders(now,timeout,res);
        }
        return res;
    }

    public SubscriberConf getSubscriberConf() {
        return subscriberConf;
    }

    public PhysicalTransport getTrans() {
        return channelDispatcher.trans;
    }

    public void setSubscriberConf(SubscriberConf subscriberConf) {
        this.subscriberConf = subscriberConf;
        if ( subscriberConf != null ) {
            hbTimeoutMS = subscriberConf.getSenderHBTimeout();
        }
    }

    public boolean isUnordered() {
//        if (subscriberConf != null )
//            return subscriberConf.isUnreliable();
        return false;
    }

    public TransportDriver getChannelDispatcher() {
        return channelDispatcher;
    }

    public void setChannelDispatcher(TransportDriver channelDispatcher) {
        this.channelDispatcher = channelDispatcher;
    }

    public boolean isUnreliable() {
        if (subscriberConf != null )
            return subscriberConf.isUnreliable();
        return false;
    }

    public FCSubscriber getSubscriber() {
        return subscriber;
    }

    public void setSubscriber(FCSubscriber subs) {
        this.subscriber = subs;
    }

    public void setSender(PacketSendBuffer sender) {
        this.sender = sender;
    }

    public PacketSendBuffer getSender() {
        return sender;
    }

    public int getTopicId() {
        if ( topicId < 0 ) {
            if ( subscriberConf != null ) {
                topicId = subscriberConf.getTopicId();
            } else if ( publisherConf != null ) {
                topicId = publisherConf.getTopicId();
            }
        }
        return topicId;
    }

    public void setPublisherConf(PublisherConf publisherConf) {
        this.publisherConf = publisherConf;
    }

    public PublisherConf getPublisherConf() {
        return publisherConf;
    }

    public long getHbTimeoutMS() {
        return hbTimeoutMS;
    }
}
