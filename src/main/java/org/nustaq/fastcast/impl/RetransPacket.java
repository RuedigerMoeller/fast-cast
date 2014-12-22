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
                "seqNo=" + seqNo +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
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
