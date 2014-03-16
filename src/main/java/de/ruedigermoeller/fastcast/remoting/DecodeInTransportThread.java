package de.ruedigermoeller.fastcast.remoting;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/3/13
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
@Retention(RetentionPolicy.RUNTIME)

@Target(ElementType.TYPE)

/**
 * overrides any configuration settings and defines, that all calls are done directly in the transport thread having read
 * a package. Use with care, as even minimal delays in processing will cause packet loss (retransmission required).
 *
 * This is useful for custom filtering, e.g. a TopicService wants to process only a part of incoming calls. So incoming
 * remote calls are invoked in transport thread a messing is put on an application provided worker thread onyly if filtering does not reject the call,
 */
public @interface DecodeInTransportThread {
    boolean value();
}
