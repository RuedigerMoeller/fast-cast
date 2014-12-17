package org.nustaq.fastcast.examples.multiplestructs;

import org.nustaq.offheap.structs.FSTStruct;
import org.nustaq.offheap.structs.Templated;
import org.nustaq.offheap.structs.structtypes.StructByteString;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.util.Arrays;

/**
 * Created by ruedi on 17.12.14.
 */
public class MultipleProtocol {

    public static void initStructFactory() {
        FSTStructFactory.getInstance().registerClz(AMessage.class,OtherMessage.class,ComposedMessage.class);
    }

    public static class AMessage extends FSTStruct {

        protected int l;

        @Templated
        protected StructByteString stringArray[] = new StructByteString[] {
            new StructByteString(20), // will be copied to other elemenrs because of @Templated
            null, null, null, null,
        };

        public StructByteString stringArray(int index) {
            return stringArray[index];
        }

        public void stringArray(int index, StructByteString s ) {
            stringArray[index] = s;
        }

        public int stringArrayLen() {
            return stringArray.length;
        }

        public int getL() {
            return l;
        }

        public void setL(int l) {
            this.l = l;
        }

        @Override
        public String toString() {
            return "AMessage{" +
                       "l=" + l +
                       ", stringArray=" + stringArray(0) + ", " +stringArray(1) + ", " +stringArray(2) + ", " +stringArray(3) + ", " +stringArray(4) +"]"+
                   '}';
        }
    }

    public static class OtherMessage extends FSTStruct {

        protected int quantity;
        protected double value;

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "OtherMessage{" +
                       "quantity=" + quantity +
                       ", value=" + value +
                       '}';
        }
    }

    public static class ComposedMessage extends FSTStruct {

        protected AMessage megA = new AMessage(); // templating !
        protected OtherMessage msgB = new OtherMessage();

        public AMessage getMegA() {
            return megA;
        }

        public void setMegA(AMessage megA) {
            this.megA = megA;
        }

        public OtherMessage getMsgB() {
            return msgB;
        }

        public void setMsgB(OtherMessage msgB) {
            this.msgB = msgB;
        }

        @Override
        public String toString() {
            return "ComposedMessage{" +
                       "megA=" + megA +
                       ", msgB=" + msgB +
                       '}';
        }
    }

}
