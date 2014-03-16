package stuff;

import de.ruedigermoeller.serialization.util.FSTUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

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
 * Date: 26.10.13
 * Time: 18:23
 * To change this template use File | Settings | File Templates.
 */
public class MemBench {

    ByteBuffer underLying = ByteBuffer.allocateDirect(1000*1200);

    byte big[] = new byte[1000*1200];

    public MemBench() {
        underLying.mark();
    }

    final int CHUNK = 10;
    byte buf[] = new byte[CHUNK];
    public void loopCopy() {
        underLying.reset();
        for ( int i = 0; i < 1200; i++ ) {
            int pos = i * 1000;
            underLying.get(buf,0,4);
            if ( un.getInt(buf,FSTUtil.bufoff)== 12 ) {
                System.out.println("POK");
            }
            underLying.position(pos);
        }
    }

    Unsafe un = FSTUtil.getUnsafe();

    public void loopheap() {
        for ( int i = 0; i < 1200; i++ ) {
            int pos = i * 1000;
            if (  un.getInt(big,pos+FSTUtil.bufoff) == 12 ) {
                System.out.println("POK");
            }
        }
    }

    public void loop() {
        underLying.reset();
        for ( int i = 0; i < 1200; i++ ) {
            int pos = i * 1000;
            if ( underLying.getInt(pos) == 12 ) {
                System.out.println("POK");
            }
            underLying.position(pos);
        }
    }

    public static void main(String arg[]) {
        int N = 1000000;
        MemBench b = new MemBench();

        long tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loop();
        }
        System.out.println("struct access in direct buffer"+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loopCopy();
        }
        System.out.println("copy "+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loopheap();
        }
        System.out.println("heap "+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loop();
        }
        System.out.println("direct "+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loopCopy();
        }
        System.out.println("copy "+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loopheap();
        }
        System.out.println("heap "+(System.currentTimeMillis()-tim));

        tim = System.currentTimeMillis();
        for ( int i=0; i < N; i++ ) {
            b.loopheap();
        }
        System.out.println("heap "+(System.currentTimeMillis()-tim));
    }
}
