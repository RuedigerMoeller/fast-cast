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
