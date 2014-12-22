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


import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.structtypes.StructString;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/10/13
 * Time: 11:43 PM
 * superclass of all packets sent.
 * ATTENTION: This is a struct class, runtime byte code instrumentation will layout this in a flat manner
 * on top a Bytez instance.
 */
public class Packet extends FSTStruct {

    public static final int MAX_NODE_NAME_LEN = 10;
    protected StructString receiver = new StructString(MAX_NODE_NAME_LEN);
    protected StructString sender = new StructString(MAX_NODE_NAME_LEN);
    protected int topic;
    protected long seqNo;

    public StructString getSender() {
        return sender;
    }

    public void setSender(StructString sender) {
        this.sender = sender;
    }

    public StructString getReceiver() {
        return receiver;
    }

    public void setReceiver(StructString receiver) {
        this.receiver = receiver;
    }

    public int getTopic() {
        return topic;
    }

    public void setTopic(int topic) {
        this.topic = topic;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "seqNo=" + seqNo +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
                '}';
    }

}
