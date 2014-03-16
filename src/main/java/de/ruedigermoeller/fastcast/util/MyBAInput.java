package de.ruedigermoeller.fastcast.util;

import java.io.*;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
 * <p/>
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * <p/>
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 * <p/>
 * Date: 29.01.13
 * Time: 21:38
 * To change this template use File | Settings | File Templates.
 */
// unsynchronized version, public byte array
public class MyBAInput extends ByteArrayInputStream {

    public MyBAInput(byte[] buf) {
        super(buf);
    }

    public MyBAInput(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] b) {
        buf = b;
        pos = 0;
        mark = 0;
        count = b.length;
    }

    public void setBuf(byte[] b, int start) {
        buf = b;
        pos = start;
        mark = 0;
        count = b.length;
    }

    public void setPos(int p) {
        pos = p;
    }

    public int read() {
        return buf[pos++] & 0xff;
    }

    public int read(byte b[], int off, int len) {
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    public final short readShort() {
        int ch1 = (buf[pos++]+256)&0xff;
        int ch2 = (buf[pos++]+256)&0xff;
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public final int readInt() {
        int ch1 = (buf[pos++]+256)&0xff;
        int ch2 = (buf[pos++]+256)&0xff;
        int ch3 = (buf[pos++]+256)&0xff;
        int ch4 = (buf[pos++]+256)&0xff;
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public final long readLong() {
        return (((long)buf[pos++] << 56) +
                ((long)(buf[pos++] & 255) << 48) +
                ((long)(buf[pos++] & 255) << 40) +
                ((long)(buf[pos++] & 255) << 32) +
                ((long)(buf[pos++] & 255) << 24) +
                ((buf[pos++] & 255) << 16) +
                ((buf[pos++] & 255) <<  8) +
                ((buf[pos++] & 255) <<  0));
    }

}

