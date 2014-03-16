package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.heapoff.bytez.Bytez;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/12/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FCBinaryMessageListener {

    public void receiveBinary(Bytez bytes, int offset, int length);

}
