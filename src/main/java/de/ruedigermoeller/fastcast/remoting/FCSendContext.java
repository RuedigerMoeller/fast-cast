package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/8/13
 * Time: 11:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class FCSendContext {

    private long longFlowHeader;

    public static FCSendContext get() {
        return context.get();
    }

    static ThreadLocal<FCSendContext> context = new ThreadLocal<FCSendContext>() {
        @Override
        protected FCSendContext initialValue() {
            return new FCSendContext();
        }
    };

    String receiver;
    byte flowHeader[];

    public byte[] getFlowHeader() {
        return flowHeader;
    }

    /**
     * set a tag which can be used by the FSTTopicService.readAndFilter method for fast filtering
     * @param flowHeader
     */
    public void setFlowHeader(byte[] flowHeader) {
        this.flowHeader = flowHeader;
    }

    Bytez intTmp = new HeapBytez(new byte[4]); // cache
    Bytez longTmp = new HeapBytez(new byte[8]); // cache

    public void setIntFlowHeader(int flowHeader) {
        intTmp.putInt(0, flowHeader);
        this.flowHeader = intTmp.asByteArray();
    }

    public void setLongFlowHeader(long longFlowHeader) {
        longTmp.putLong(0, longFlowHeader);
        this.flowHeader = longTmp.asByteArray();
    }

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public void reset() {
        receiver = null;
        flowHeader = null;
    }

}
