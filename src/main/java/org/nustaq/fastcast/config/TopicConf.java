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
package org.nustaq.fastcast.config;

/**
 * Created by ruedi on 04.12.14.
 */
public class TopicConf {

    private int id;      // will be promoted to pub/sub conf [is redundant]
    private String name;      // symbolic, for lookup

    private PublisherConf pub;
    private SubscriberConf sub;

    public int getId() {
        return id;
    }

    public TopicConf id(int id) {
        this.id = id;
        validateAfterRead();
        return this;
    }

    public String getName() {
        return name;
    }

    public TopicConf name(String name) {
        this.name = name;
        return this;
    }

    public PublisherConf getPublisher() {
        return pub;
    }

    public TopicConf publisher(PublisherConf pub) {
        this.pub = pub;
        validateAfterRead();
        return this;
    }

    public SubscriberConf getSub() {
        return sub;
    }

    public TopicConf subscriber(SubscriberConf sub) {
        this.sub = sub;
        validateAfterRead();
        return this;
    }

    public void validateAfterRead() {
        if ( pub != null )
            pub.topicId = id;
        if ( sub != null )
            sub.topicId = id;
    }
}
