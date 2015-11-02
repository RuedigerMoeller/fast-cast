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

import org.nustaq.fastcast.impl.Topic;
import org.nustaq.fastcast.transport.PhysicalTransport;
import org.nustaq.kson.Kson;
import org.nustaq.kson.KsonTypeMapper;

import java.io.File;

/**
 * Created by ruedi on 04.12.14.
 */
public class ClusterConf {
    public PhysicalTransportConf transports[];
    public TopicConf topics[];

    public PhysicalTransportConf[] getTransports() {
        return transports;
    }

    public ClusterConf transports(PhysicalTransportConf ... transports) {
        this.transports = transports;
        return this;
    }

    public TopicConf[] getTopics() {
        return topics;
    }

    public ClusterConf topics(TopicConf ... topics) {
        this.topics = topics;
        return this;
    }

    public TopicConf getTopic(String name) {
        for (int i = 0; i < topics.length; i++) {
            TopicConf topic = topics[i];
            if ( topic.getName() == null )
                throw new RuntimeException("unnamed topic. Please ensure each topic has a name assigned");
            if ( name.equalsIgnoreCase(topic.getName())) {
                topic.validateAfterRead();
                return topic;
            }
        }
        return null;
    }

    public static synchronized ClusterConf readFrom( String filePath ) throws Exception {
        return (ClusterConf) new Kson()
               .map(PublisherConf.class, SubscriberConf.class, TopicConf.class, ClusterConf.class)
               .readObject(new File(filePath));
    }

}
