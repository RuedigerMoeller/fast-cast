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
