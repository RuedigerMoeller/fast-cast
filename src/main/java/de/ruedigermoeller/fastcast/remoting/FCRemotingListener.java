package de.ruedigermoeller.fastcast.remoting;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/19/13
 * Time: 5:50 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FCRemotingListener {

    /**
     * signaled if this service could not keep up with send rate and requested
     * retransmission of a packet which is already removed from the senders history.
     *
     * This might also happen to senders if they could not process incoming call results fast enough or
     * there are network issues/congestion
     *
     * Reasons are: 1) history buffer of sender is too small
     *              2) network congestions/overload
     *              3) (most frequent) receiver is slow or has long GC pauses
     * after this message, no further messages will be received until
     * reconnects.
     */
    public void droppedFromTopic( int topicId, String topicName );

    /**
     * called if no heartbeats on topic are received from a sender. This happens
     * if sender is down or has massive GC pauses (larger 30000). This message can be pretty late
     * (30 seconds).
     *
     * Use FCMembershipService in order to get near time notifications on new/leaving cluster members on a topic.
     * @param sender
     */
    public void senderDied( int topicId, String topicName, String sender);

    /**
     * called if a new sender joined this topic
     *
     * @param newSender - node Id of new sender
     * @param seqNo - first sequence number accepted
     */
    public void senderBootstrapped(int topic, String topicName, String newSender, long seqNo);



}
