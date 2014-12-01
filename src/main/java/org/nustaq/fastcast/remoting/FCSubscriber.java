package org.nustaq.fastcast.remoting;

import org.nustaq.offheap.bytez.ByteSource;
import org.nustaq.offheap.bytez.Bytez;

/**
 * Created by ruedi on 29.11.2014.
 */
public interface FCSubscriber {

    public void messageReceived(String sender, long sequence, Bytez b, long off, int len);

    /**
     * called in case the receiver was too slow in processing messages
     * and therefore got dropped (unrecoverable message loss).
     */
    public void dropped();

    /**
     * a sender stopped sending or terminated
     * @param senderNodeId
     */
    public void senderTerminated( String senderNodeId );

    /**
     * called upon the first message regulary received from a sender
     * @param receivesFrom
     * @param seqNo
     */
    public void senderBootstrapped(String receivesFrom, long seqNo);
}
