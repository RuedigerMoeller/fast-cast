package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.serialization.FSTObjectInput;

import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/20/13
 * Time: 1:31 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FCInvoker {
    public void invoke(Object target, FSTObjectInput in);
}