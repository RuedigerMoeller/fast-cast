package org.nustaq.fastcast.impl;


import org.nustaq.offheap.structs.Templated;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/10/13
 * Time: 11:50 PM
 * Retransmission request. Contains an array of sequence intervals.
 * 
 * ATTENTION: this is a struct class.
 */
public class RetransPacket extends Packet {

    // 21 entries struct template
    @Templated
    protected RetransEntry[] retransEntries = {
        new RetransEntry(),
        null,null,null,null,null,
        null,null,null,null,null,

        null,null,null,null,null,
        null,null,null,null,null,

        null,null,null,null,null,
        null,null,null,null,null,

        null,null,null,null,null,
        null,null,null,null,null,
    };

    protected int retransIndex;

    public int retransEntriesLen() {
        return retransEntries.length;
    }

    public RetransEntry retransEntries(int i) {
        return retransEntries[i];
    }

    public RetransEntry current() {
        return retransEntries(retransIndex);
    }

    public void nextEntry() {
        retransIndex++;
    }

    public int getRetransIndex() {
        return retransIndex;
    }

    public void setRetransIndex(int retransIndex) {
        this.retransIndex = retransIndex;
    }

    public void clear() {
        setRetransIndex(0);
    }

    public boolean isFull() {
        return getRetransIndex() >= retransEntriesLen()-1;
    }

    @Override
    public String toString() {
        return "RetransPacket{" +
                "sent=" + sent +
                ", seqNo=" + seqNo +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
                ", cluster=" + cluster +
                ", retransEntries=" + entriesString() +
                ", retransIndex=" + retransIndex +
                '}';
    }

    protected String entriesString() {
        String res = "[";
        for ( int n = 0; n < getRetransIndex(); n++ ) {
            RetransEntry retransEntry = retransEntries(n);
            res+="[ "+ retransEntry.getFrom()+","+retransEntry.getTo()+"] ";
        }
        return res+"]";
    }

    public int computeNumPackets() {
        int res = 0;
        for (int i=0; i < retransIndex; i++)
            res += retransEntries(i).getNumPackets();
        return res;
    }
}
