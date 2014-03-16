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
 * Date: 07.08.13
 * Time: 19:53
 * To change this template use File | Settings | File Templates.
 */
public class FCReceiveContext {

    public static FCReceiveContext get() {
        return context.get();
    }

    static ThreadLocal<FCReceiveContext> context = new ThreadLocal<FCReceiveContext>() {
        @Override
        protected FCReceiveContext initialValue() {
            return new FCReceiveContext();
        }
    };

    TopicEntry entry;
    String sender;

    public void setEntry(TopicEntry entry) {
        this.entry = entry;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSender() {
        return sender;
    }

    public TopicEntry getEntry() {
        return entry;
    }
}
