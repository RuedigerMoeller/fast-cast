package org.nustaq.fastcast.impl;

import org.nustaq.offheap.structs.FSTStruct;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/12/13
 * Time: 8:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class RetransEntry extends FSTStruct {
    protected long from;
    protected long to;

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "RetransEntry{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }

    public long getNumPackets() {
        return to-from;
    }
}
