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

    public void setId(int id) {
        this.id = id;
        validateAfterRead();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PublisherConf getPub() {
        return pub;
    }

    public void setPub(PublisherConf pub) {
        this.pub = pub;
    }

    public SubscriberConf getSub() {
        return sub;
    }

    public void setSub(SubscriberConf sub) {
        this.sub = sub;
    }

    public void validateAfterRead() {
        if ( pub != null )
            pub.topicId = id;
        if ( sub != null )
            sub.topicId = id;
    }
}
