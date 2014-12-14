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
