package org.nustaq.fastcast.impl;


import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.offheap.bytez.onheap.HeapBytez;

/*
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/11/13
 * Time: 1:20 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * Manages defragmentation of large messages + splitting of multiple messages per packet
 */
public class Defragmenter implements ByteArrayReceiver {
    final int STATE_CHAIN = 1;
    final int STATE_COMPLETE = 0;

    int state = STATE_COMPLETE;

    byte buf[] = new byte[500];
    int bufIndex = 0;

    @Override
    public void receiveChunk(long sequence, Bytez b, int off, int len, boolean complete) {
        switch ( state ) {
            case STATE_COMPLETE:
                if ( complete ) {
                    msgDone(sequence, b, off, len);
                    break;
                } else {
                    state = STATE_CHAIN;
                }
                // fall through Fixme: can do zerocopy in this case
            case STATE_CHAIN:
                if ( len+bufIndex >= buf.length ) {
                    byte old[] = buf;
                    buf = new byte[len+bufIndex];
                    System.arraycopy(old,0,buf,0,bufIndex);
                }
                try {
                    if ( len > 0 ) {
                        //System.arraycopy(b,off,buf,bufIndex,len);
                        b.getArr(off,buf,bufIndex,len);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                bufIndex+=len;
                if ( complete ) {
                    msgDone(sequence,new HeapBytez(buf),0,bufIndex); // FIXME alloc
                    state = STATE_COMPLETE;
                    bufIndex = 0;
                }
                break;
        }
    }

    public void msgDone(long sequence, Bytez b, int off, int len) {
        // pls override
        System.out.println("received byte array "+len);
    }
}
