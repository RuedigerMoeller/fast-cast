package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.packeting.TopicEntry;

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
 * Time: 21:04
 * To change this template use File | Settings | File Templates.
 */
public abstract class FCFutureResultHandler<T> {

    public static FCFutureResultHandler NULL = new FCFutureResultHandler() {
        @Override
        public void resultReceived(Object obj, String sender) {
        }
    };

    long cbid;
    volatile TopicEntry topicEntry;

    public abstract void resultReceived(T obj, String sender);
    public void timeoutReached() { }
    public void sendResult(T obj) { throw new RuntimeException("only invokable at callee side"); }
    public void done() {
        if (topicEntry == null )
            throw new RuntimeException("only invokable at caller side");
        topicEntry.getCbMap().freeCallbackId(cbid);
    }

    public long getCbid() {
        return cbid;
    }

    public void setCbid(long cbid) {
        this.cbid = cbid;
    }

    public void extendTimeout() {
        if ( topicEntry == null )
            throw new RuntimeException("only invokable at caller side");
        topicEntry.getCbMap().extendTimeout(cbid);
    }

    public void setTopicEntry(TopicEntry topicEntry) {
        this.topicEntry = topicEntry;
    }

    @Override
    public String toString() {
        return "FCFutureResultHandler{" +
                "cbid=" + getCbid() +
                ", topicEntry=" + topicEntry +
                '}';
    }
}
