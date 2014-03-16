package de.ruedigermoeller.fastcast.remoting;

import java.lang.annotation.*;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/3/13
 * Time: 4:30 PM
 * To change this template use File | Settings | File Templates.
 */
@Retention(RetentionPolicy.RUNTIME)

@Target(ElementType.TYPE)
@Inherited

/**
 * Overrides any configuration setting. In Many-To-One processing patterns, process each messages on an an own thread for
 * each sender. This requires the TopicService Implementation to be threadsafe !
 *
 * if this is set to false: Use one worker thread for this topicservice (single threaded)
 *
 * A possibility to get concurrent decoding and single threaded processing is to set this to true and synchronize methods. This
 * way decoding of messages is still concurrent, but execution not (pays of in case of Many-To-One and if decoding is more expensive than
 * actual work done in a method (which is frequently true)
 *
 * @DecodeInTransportThread = true nullifies this, as messages are directly delivered from transport then.
 */
public @interface PerSenderThread {
    boolean value();
}
