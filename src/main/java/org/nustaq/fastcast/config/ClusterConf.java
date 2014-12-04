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

    public static ClusterConf readFrom( String filePath ) throws Exception {
        return (ClusterConf) new Kson()
               .map(PublisherConf.class, SubscriberConf.class, TopicConf.class, ClusterConf.class)
               .readObject(new File(filePath));
    }

    public static void main(String a[]) throws Exception {
        ClusterConf clusterConf = readFrom("/home/ruedi/IdeaProjects/fast-cast/src/main/conf/cluster.conf");
        System.out.println(clusterConf);
    }
}
