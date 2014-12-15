package org.nustaq.fastcast.examples.structencoding;

import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.FSTStructAllocator;
import org.nustaq.offheap.structs.Templated;
import org.nustaq.offheap.structs.structtypes.StructByteString;
import org.nustaq.offheap.structs.structtypes.StructString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

/**
 * Created by moelrue on 12/15/14.
 */
public class Protocol {

    public static class InstrumentStruct extends FSTStruct {

        protected StructByteString mnemonic = new StructByteString(5);
        protected long instrumentId;

        public StructByteString getMnemonic() {
            return mnemonic;
        }

        public void setMnemonic(StructByteString mnemonic) {
            this.mnemonic = mnemonic;
        }

        public long getInstrumentId() {
            return instrumentId;
        }

        public void setInstrumentId(long instrumentId) {
            this.instrumentId = instrumentId;
        }

        @Override
        public String toString() {
            return "InstrumentStruct{" +
                    "mnemonic=" + mnemonic +
                    ", instrumentId=" + instrumentId +
                    '}';
        }
    }

    public static class PriceUpdateStruct extends FSTStruct {

        protected InstrumentStruct instrument = new InstrumentStruct();
        protected int qty = 0;
        protected double prc;

        public InstrumentStruct getInstrument() {
            return instrument;
        }

        public void setInstrument(InstrumentStruct instrument) {
            this.instrument = instrument;
        }

        public int getQty() {
            return qty;
        }

        public void setQty(int qty) {
            this.qty = qty;
        }

        public double getPrc() {
            return prc;
        }

        public void setPrc(double prc) {
            this.prc = prc;
        }

        @Override
        public String toString() {
            return "PriceUpdateStruct{" +
                    "instrument=" + instrument +
                    ", qty=" + qty +
                    ", prc=" + prc +
                    '}';
        }
    }

    public static void initStructFactory() {
        FSTStructFactory.getInstance().registerClz(InstrumentStruct.class,PriceUpdateStruct.class);
    }

    private static void fillStruct(PriceUpdateStruct msg) {
        InstrumentStruct instrument = msg.getInstrument();
        instrument.getMnemonic().setString("BMW");
        instrument.setInstrumentId(13);
        msg.setPrc(99.0);
        msg.setQty(100);
    }

    public static void main(String s[]) {

        initStructFactory();

        PriceUpdateStruct template = new PriceUpdateStruct();
        FSTStructAllocator onHeapAlloc = new FSTStructAllocator(0);

        template = onHeapAlloc.newStruct(template); // speed up instantiation by moving template also off heap

        PriceUpdateStruct newStruct = onHeapAlloc.newStruct(template);
        int sizeOf = newStruct.getByteSize();

        // demonstrates that theoretical send rate is >20 millions messages per second on
        // an I7 box
        byte networkBuffer[] = new byte[template.getByteSize()];

        while ( true ) {
            long tim = System.currentTimeMillis();
            for ( int i = 0; i < 20_000_000; i++ ) {
                fillStruct(newStruct);
                // emulate network sending by copying to buffer
                newStruct.getBase().getArr(newStruct.getOffset(),networkBuffer,0,sizeOf);
            }
            System.out.println("tim: "+(System.currentTimeMillis()-tim));
        }

//        System.out.println(newStruct);
//        System.out.println("size:" + newStruct.getByteSize());

    }

}
