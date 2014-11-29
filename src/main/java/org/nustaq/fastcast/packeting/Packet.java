package org.nustaq.fastcast.packeting;


import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.structtypes.StructString;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/10/13
 * Time: 11:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class Packet extends FSTStruct {

    protected StructString cluster = new StructString(8);
    protected StructString receiver = new StructString(15);
    protected StructString sender = new StructString(15);
    protected int topic;
    protected long seqNo;
    protected long sent;
    protected int sendPauseSender;

    public long getSent() {
        return sent;
    }

    public void setSent(long sent) {
        this.sent = sent;
    }

    public StructString getCluster() {
        return cluster;
    }

    public void setCluster(StructString cluster) {
        this.cluster = cluster;
    }

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
                "sent=" + sent +
                ", seqNo=" + seqNo +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
                ", cluster=" + cluster +
                '}';
    }

    public void setSendPauseSender(int sendPauseSender) {
        this.sendPauseSender = sendPauseSender;
    }

    public int getSendPauseSender() {
        return sendPauseSender;
    }
}
