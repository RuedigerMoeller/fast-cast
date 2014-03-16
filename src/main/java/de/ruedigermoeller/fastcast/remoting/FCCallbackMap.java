package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.util.FCLog;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 * Date: 08.08.13
 * Time: 21:02
 * To change this template use File | Settings | File Templates.
 */
//FIXME: do cyclic recount to avoid leaking currentOpen counter long term
public class FCCallbackMap {

    int linesize =1000;
    long timeout = 5000;
    int maxOpen;
    AtomicLong curId = new AtomicLong(1);
    AtomicInteger currentOpen = new AtomicInteger(0);

    LinkedList<CBLine> lines;
    int dequeCapacity;

    ConcurrentHashMap<Long,FCFutureResultHandler> extendedTimeouts0 = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long,FCFutureResultHandler> extendedTimeouts1 = new ConcurrentHashMap<>();
    long lastFlip = System.currentTimeMillis();

    public FCCallbackMap(int size, int timeOut) {
        this.timeout = timeOut;
        maxOpen = size;
        linesize = Math.max(maxOpen/10,10);
        dequeCapacity = size/ linesize +2;
        lines = new LinkedList<>();
    }

    public long assignCallbackId(FCFutureResultHandler res) {
        while (currentOpen.get()>maxOpen) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                FCLog.log(e);
            }
        }
        long id = curId.incrementAndGet();
        CBLine curLine = null;
        synchronized (lines) {
            if ( lines.size() == 0 )
                roll();
            curLine = lines.getFirst();
            if (!curLine.fits(id)) {
                roll();
                curLine = lines.getFirst();
            }
            try {
                curLine.assignCallback(id,res);
            } catch (ArrayIndexOutOfBoundsException e ) {
                FCLog.get().warn(e);
                return assignCallbackId(res);
            }
        }
        currentOpen.addAndGet(1);
        return id;
    }

    CBLine newLine(long tim, int linesize, long offset) {
        return new CBLine(tim, linesize, curId.get());
    }

    void roll() {
        long now = System.currentTimeMillis();
        lines.addFirst(newLine(now, linesize, curId.get()));
    }

    public FCFutureResultHandler get(long id) {
        if ( extendedTimeouts0.size() > 0 || extendedTimeouts1.size() > 0 ) {
            Long lid = id;
            FCFutureResultHandler res = extendedTimeouts0.get(lid);
            if ( res == null ) {
                res = extendedTimeouts1.get(lid);
            }
            if ( res != null ) {
                return res;
            }
        }
        synchronized (lines) {
            if (lines.size()==0) {
                return null;
            }
            for (CBLine line : lines ) {
                if ( line.fits(id) ) {
                    return line.get(id);
                }
            }
        }
//        System.out.println("missed callback "+id);
        return null;
    }

    public void freeCallbackId(long id) {

        if ( extendedTimeouts0.size() > 0 || extendedTimeouts1.size() > 0 ) {
            Long lid = id;
            extendedTimeouts0.remove(lid);
            extendedTimeouts1.remove(lid);
        }
        freeRegularCallbackId(id);
    }

    private void freeRegularCallbackId(long id) {
        synchronized (lines) {
            if (lines.size()==0) {
                return;
            }
            for (CBLine line : lines ) {
                if ( line.fits(id) ) {
                    line.clear(id);
                    currentOpen.addAndGet(-1);
                    return;
                }
            }
            FCLog.get().warn("could not free cbid");
        }
    }

    // called cyclic
    public void release(long nowMillis) {
        if ( nowMillis - lastFlip > timeout ) {
            flip();
            lastFlip = nowMillis;
        }
        synchronized (lines) {
            if (lines.size() == 0 )
                return;
            if ( nowMillis - lines.getFirst().creationTime > timeout ) {
                roll();
            }
            while (lines.size() > 0 && nowMillis - lines.getLast().creationTime > timeout*2 ) {
//                System.out.println("released line "+(nowMillis - lines.getLast().creationTime)+" siz "+lines.size());
                CBLine cbLine = lines.removeLast();
                FCFutureResultHandler[] results = cbLine.results;
                for (int i = 0; i < results.length; i++) {
                    FCFutureResultHandler result = results[i];
                    if ( result != null ) {
                        result.timeoutReached();
                        currentOpen.addAndGet(-1);
                        results[i] = null;
                    }
                }
            }
        }
    }

    // extended timout only
    private void flip() {
        ConcurrentHashMap<Long, FCFutureResultHandler> tmp = extendedTimeouts1;
        extendedTimeouts1 = extendedTimeouts0;
        for (Iterator<Map.Entry<Long, FCFutureResultHandler>> iterator = tmp.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<Long, FCFutureResultHandler> next = (Map.Entry<Long, FCFutureResultHandler>) iterator.next();
            if ( !extendedTimeouts1.containsKey(next.getKey())) {
                next.getValue().timeoutReached();
            }
        }
        tmp.clear();
        extendedTimeouts0 = tmp;
    }

    /**
     * caution: very expensive, not for mass usage
     * @param cbid
     */
    public void extendTimeout(long cbid) {
        FCFutureResultHandler value = get(cbid);
        if ( value == null ) {
            throw new RuntimeException("callback already timed out. override FCFutureResultHandler.timedOut to detect this");
        }
        extendedTimeouts0.put(cbid, value);
        freeRegularCallbackId(cbid);
    }

    static class CBLine {
        FCFutureResultHandler results[];
        long cbIdOffset;
        long creationTime;

        CBLine(long time, int lineSize, long offset) {
            results = new FCFutureResultHandler[lineSize];
            cbIdOffset = offset;
            creationTime = time;
        }

        boolean fits(long id) {
            return id >= cbIdOffset && id-cbIdOffset<results.length;
        }

        void assignCallback( long id, FCFutureResultHandler res ) {
            results[((int) (id - cbIdOffset))] = res;
        }

        public FCFutureResultHandler get(long id) {
            return results[((int) (id - cbIdOffset))];
        }

        public void clear(long id) {
            results[((int) (id - cbIdOffset))] = null;
        }
    }

    public static void main(String arg[]) {
        final FCCallbackMap map = new FCCallbackMap(1000,5000);

        new Thread("releaser") {
            public void run() {
                while(true) {
                    map.release(System.currentTimeMillis());
                }
            }
        }.start();

        final AtomicInteger open = new AtomicInteger(0);
        while( true ) {
            open.addAndGet(1);
            map.assignCallbackId(new FCFutureResultHandler() {
                long time = System.currentTimeMillis();

                @Override
                public void resultReceived(Object obj, String sender) {

                }

                @Override
                public void timeoutReached() {
                    open.decrementAndGet();
                    System.out.println("tout "+(System.currentTimeMillis()-time)+" open "+open);
                }
            });
        }

    }

}
