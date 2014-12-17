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
