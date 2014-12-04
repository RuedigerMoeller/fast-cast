package org.nustaq.fastcast.impl;

import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.api.*;
import org.nustaq.fastcast.transport.PhysicalTransport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

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
    SubscriberConf receiverConf;

    TransportDriver channelDispatcher;
    PacketSendBuffer sender;
    TopicStats stats;
    ConcurrentHashMap<String,Long> senderHeartbeat = new ConcurrentHashMap<String, Long>();

    boolean isUnordered = false;
    boolean isUnreliable = false;
    volatile boolean listenCalls = false; // if false => only listen to call results if any
    private FCSubscriber subscriber;
    int topicId = -1;
    private long hbTimeoutMS = 3000; // dev

    public Topic(SubscriberConf receiverConf, PublisherConf publisherConf) {
        this.receiverConf = receiverConf;
        this.publisherConf = publisherConf;
        if ( receiverConf != null ) {
            hbTimeoutMS = receiverConf.getSenderHBTimeout();
        }
    }

    public void registerHeartBeat(String sender, long time) {
        senderHeartbeat.put(sender,time);
    }

    public boolean hadHeartbeat(String sender) {
        return senderHeartbeat.containsKey(sender);
    }

    public List<String> getTimedOutSenders(long now, long timeout) {
        List<String> res = new ArrayList<String>();
        for (Iterator<String> iterator = senderHeartbeat.keySet().iterator(); iterator.hasNext(); ) {
            String next = iterator.next();
            long tim = senderHeartbeat.get(next);
            if ( now-tim > timeout ) {
                res.add(next);
            }
        }
        return res;
    }

    public SubscriberConf getReceiverConf() {
        return receiverConf;
    }

    public PhysicalTransport getTrans() {
        return channelDispatcher.trans;
    }

    public void setReceiverConf(SubscriberConf receiverConf) {
        this.receiverConf = receiverConf;
    }

    public boolean isUnordered() {
        return isUnordered;
    }

    public TransportDriver getChannelDispatcher() {
        return channelDispatcher;
    }

    public void setChannelDispatcher(TransportDriver channelDispatcher) {
        this.channelDispatcher = channelDispatcher;
    }

    public boolean isUnreliable() {
        return isUnreliable;
    }

    public void setUnreliable(boolean unreliable) {
        isUnreliable = unreliable;
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
            if ( receiverConf != null ) {
                topicId = receiverConf.getTopicId();
            } else if ( publisherConf != null ) {
                topicId = publisherConf.getTopicId();
            }
        }
        return topicId;
    }

    public TopicStats getStats() {
        if ( stats == null )
            stats = new TopicStats(getTrans().getConf().getDgramsize());
        return stats;
    }

    public void removeSenders(List<String> timedOutSenders) {
        for ( String s : timedOutSenders ) {
            senderHeartbeat.remove(s);
        }
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
