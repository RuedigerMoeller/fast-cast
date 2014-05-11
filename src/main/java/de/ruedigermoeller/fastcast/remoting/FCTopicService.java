package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.serialization.FSTObjectInput;

import java.lang.reflect.Method;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
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
 * Date: 07.08.13
 * Time: 20:07
 * To change this template use File | Settings | File Templates.
 */

/**
 * for each network topic (ordered stream of messages) exactly one service exists.
 */
public class FCTopicService {

    protected String nodeId;
    protected FCRemoting remoting;
    protected String topicName;
    protected int topicNum;

    /**
     * called after network join, still not being wired to receive messages.
     * Do not override this, override init instead.
     * @param fc
     * @param nodeId
     * @param topicName
     * @param topic
     */
    void initializeBeforeListening(FastCast fc, String nodeId, String topicName, int topic) {
        this.nodeId = nodeId;
        this.remoting = fc;
        this.topicName = topicName;
        this.topicNum = topic;
    }

    /**
     * get unique number of this topic
     * @return
     */
    public int getTopicNum() {
        return topicNum;
    }

    /**
     * called right before decoding, return true to consume the call.
     * If you read from the object stream, you need to consume or rewind the
     * stream to its original position.
     * @param methodIndex
     * @param m
     * @param in
     * @param types
     * @return
     */
    protected boolean invoke(int methodIndex, Method m, FSTObjectInput in, Class types[]) {
        return false;
    }

    /**
     * @return the cluster unique id of this process
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return the remoting instance. Allows access to other services etc.
     */
    public FCRemoting getRemoting() {
        return remoting;
    }

    /**
     * @return unque name of this topic
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * signaled if this service could not keep up with send rate and requested
     * retransmission of a packet which is already removed from the senders history.
     * Reasons are: 1) history buffer of sender is too small
     *              2) network congestions/overload
     *              3) (most frequent) receiver is slow or has long GC pauses
     * after this message, no further messages will be received until receivers
     * reconnects.
     */
    public void droppedFromReceiving() {
        FCLog.get().fatal("topic receiver " + getTopicName() + " has been dropped");
        System.exit(1);
    }

    /**
     * override to do init and stuff
     */
    public void init() {
    }

    /**
     * called is pure binary content has been sent, bypass of remoting layer
     * @param bytes
     * @param offset
     * @param length
     */
    @RemoteMethod(-1) // internal flag to bypass marshalling, do not use negative indizes in your code
    public void receiveBinary(Bytez bytes, int offset, int length) {
        // do nothing
    }

    /**
     * next remote call goes to sender of current method.
     */
    public void replyToSender() {
        FCSendContext.get().setReceiver(FCReceiveContext.get().getSender());
    }

    /**
     * allows to filter out messages before they are decoded by reading a user provided header (see writeFilterBytes). return -1
     * to ignore the message or the size of the application provided header.
     * @param bytez
     * @param offset
     * @return
     */
    protected int readAndFilter(int methodId, Bytez bytez, int offset) {
        return 0;
    }
}
