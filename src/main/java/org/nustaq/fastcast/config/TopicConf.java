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
