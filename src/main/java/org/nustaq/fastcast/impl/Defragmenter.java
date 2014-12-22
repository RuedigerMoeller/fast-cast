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
    HeapBytez bufBytez = new HeapBytez(buf,0,0);
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
                    buf = new byte[Math.max(len+bufIndex,buf.length*2)];
                    System.arraycopy(old,0,buf,0,bufIndex);
//                    System.out.println("POK increase buffer to"+buf.length);
                }
                try {
                    if ( len > 0 ) {
                        //System.arraycopy(b,off,buf,bufIndex,len);
                        b.getArr(off,buf,bufIndex,len);
//                        System.out.println("POK chain copy "+len);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                bufIndex+=len;
                if ( complete ) {
                    bufBytez.setBase(buf,0,bufIndex);
                    msgDone(sequence,bufBytez,0,bufIndex);
                    state = STATE_COMPLETE;
                    bufIndex = 0;
                }
                break;
        }
    }

    public void msgDone(long sequence, Bytez b, int off, int len) {
        System.out.println("received byte array "+len);
    }
}
