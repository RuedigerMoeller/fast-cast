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
import org.nustaq.offheap.structs.FSTStructAllocator;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/10/13
 * Time: 11:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataPacket extends Packet {

    public static DataPacket getTemplate(int packetSize) {
        int emptyPackSize = new FSTStructAllocator(1).newStruct(new DataPacket()).getByteSize();
        DataPacket template = new DataPacket();
        int payMaxLen = packetSize - emptyPackSize - 2;
        template.data = new byte[payMaxLen];
        return template;
    }

    public static final short COMPLETE = 1; // payload fits inpacket
    public static final short CHAINED = 2;  // payload is chained. len then denotes len in current packet
    public static final short EOP = 3;      // end of packet
    public static final short MAX_CODE = EOP;  //

    public static final int HEADERLEN = 4;

    protected boolean isDecoded;
    protected boolean isRetrans = false;
    protected int bytesLeft;
    protected byte[] data = new byte[0];

    public void data(int index, byte val) {
        data[index] = val;
    }

    public byte data(int index) {
        return data[index];
    }

    public int dataLen() {
        return data.length;
    }

    public int dataIndex() {
        // generated
        return -1;
    }

    /**
     * internal flag, anyway transmitted FIXME: not required anymore (relict of FC 2.x) !
     * @return
     */
    public boolean isDecoded() {
        return isDecoded;
    }

    public void setDecoded(boolean decoded) {
        isDecoded = decoded;
    }

    public void dataPointer(FSTStruct pointer) {
        // generated
    }

    public FSTStruct dataPointer() {
        // generated
        return null;
    }

    public boolean isRetrans() {
        return isRetrans;
    }

    public void setRetrans(boolean isRetrans) {
        this.isRetrans = isRetrans;
    }

    public int getBytesLeft() {
        return bytesLeft;
    }

    public int getDGramSize() {
        return getByteSize()-bytesLeft;
    }

    public void setBytesLeft(int bytesLeft) {
        this.bytesLeft = bytesLeft;
    }

    public void dumpBytes() {
        for ( int n = 0; n < dataLen()-getBytesLeft(); n++ ) {
            System.out.print(" [" + n + "], " + data(n));
        }
        System.out.println("-");
    }
    @Override
    public String toString() {
        return "DataPacket{" +
                "seqNo=" + seqNo +
                ", retr=" + isRetrans +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
                ", left=" + bytesLeft +
                ", datalen=" + dataLen() +
                ", dgsize="+getDGramSize()+
                ", decoded="+isDecoded()+
                '}';
    }


}
