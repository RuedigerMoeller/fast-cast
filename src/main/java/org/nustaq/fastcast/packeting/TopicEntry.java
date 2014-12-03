package org.nustaq.fastcast.packeting;

import org.nustaq.fastcast.config.FCPublisherConf;
import org.nustaq.fastcast.config.FCSubscriberConf;
import org.nustaq.fastcast.control.FCTransportDispatcher;
import org.nustaq.fastcast.control.FlowControl;
import org.nustaq.fastcast.remoting.*;
import org.nustaq.fastcast.transport.Transport;
import org.nustaq.fastcast.util.FCLog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
* Created with IntelliJ IDEA.
* User: ruedi
* Date: 23.08.13
* Time: 02:04
* To change this template use File | Settings | File Templates.
*/
public class TopicEntry {

    FCPublisherConf publisherConf;
    FCSubscriberConf receiverConf;

    FCTransportDispatcher channelDispatcher;
    PacketSendBuffer sender;
    TopicStats stats;
    Transport trans;
    FlowControl control;
    ConcurrentHashMap<String,Long> senderHeartbeat = new ConcurrentHashMap<String, Long>();


    boolean isUnordered = false;
    boolean isUnreliable = false;
    volatile boolean listenCalls = false; // if false => only listen to call results if any
    private FCSubscriber subscriber;
    int topicId = -1;
    private long hbTimeoutMS = 3000; // dev

    public TopicEntry(FCSubscriberConf receiverConf, FCPublisherConf publisherConf) {
        this.receiverConf = receiverConf;
        this.publisherConf = publisherConf;
        if ( publisherConf != null && publisherConf.getFlowControlClass() != null ) {
            try {
                control = (FlowControl) Class.forName(publisherConf.getFlowControlClass()).newInstance();
            } catch (Exception e) {
                FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
            }
        }
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

    public FlowControl getControl() {
        return control;
    }

    public void setControl(FlowControl control) {
        this.control = control;
    }

    public FCSubscriberConf getReceiverConf() {
        return receiverConf;
    }

    public Transport getTrans() {
        return trans;
    }

    public void setTrans(Transport trans) {
        this.trans = trans;
        stats = new TopicStats(trans.getConf().getDgramsize());
    }

    public void setReceiverConf(FCSubscriberConf receiverConf) {
        this.receiverConf = receiverConf;
    }

    public boolean isUnordered() {
        return isUnordered;
    }

    public FCTransportDispatcher getChannelDispatcher() {
        return channelDispatcher;
    }

    public void setChannelDispatcher(FCTransportDispatcher channelDispatcher) {
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
            stats = new TopicStats(((FastCast)FastCast.getFastCast()).getTransport(getReceiverConf().getTransport()).getConf().getDgramsize());
        return stats;
    }

    public void removeSenders(List<String> timedOutSenders) {
        for ( String s : timedOutSenders ) {
            senderHeartbeat.remove(s);
        }
    }

    public void setPublisherConf(FCPublisherConf publisherConf) {
        this.publisherConf = publisherConf;
    }

    public FCPublisherConf getPublisherConf() {
        return publisherConf;
    }

    public long getHbTimeoutMS() {
        return hbTimeoutMS;
    }
}
