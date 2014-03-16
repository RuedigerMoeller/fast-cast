package de.ruedigermoeller.fastcast.packeting;

import de.ruedigermoeller.heapoff.structs.FSTStruct;
import de.ruedigermoeller.heapoff.structs.FSTStructAllocator;

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

    volatile protected boolean isDecoded;
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
     * internal flag, anyway transmitted
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
                "sent=" + sent +
                ", seqNo=" + seqNo +
                ", topic=" + topic +
                ", sender=" + sender +
                ", receiver=" + receiver +
                ", cluster=" + cluster +
                ", left=" + bytesLeft +
                ", datalen=" + dataLen() +
                ", dgsize="+getDGramSize()+
                ", decoded="+isDecoded()+
                '}';
    }


}
