package org.nustaq.fastcast.api;

/*
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/8/13
 * Time: 11:47 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * ThreadLocal (see.get()) to add options to outgoing requests (currently only receiver for unicats messaging)
 */
public class FCSendContext {

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

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public void reset() {
        receiver = null;
    }

}
