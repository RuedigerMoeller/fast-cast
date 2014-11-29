package org.nustaq.fastcast.packeting;

import org.nustaq.offheap.bytez.Bytez;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/11/13
 * Time: 1:07 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ByteArrayReceiver {
    public void receiveChunk(long sequence, Bytez b, int off, int len, boolean complete);
}
