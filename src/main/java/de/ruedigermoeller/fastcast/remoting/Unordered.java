package de.ruedigermoeller.fastcast.remoting;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/18/13
 * Time: 6:41 PM
 * To change this template use File | Settings | File Templates.
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * defines that a service does not require in-order delivery. So still lost packages are guaranteed to
 * get delivered, but not in sequential order. This also means a message (method calls) can be called twice.
 * The service implementation must check by itself for that (e.g. application level timestamps or sequences)
 *
 * Typically used for hi-volume best effort data updates (e.g. stock exchange real time price feed or other
 * fine grained real time data monitoring).
 * Another use case is max throuput delivery of blobs. However since chaining is not possible unordered,
 * data must be segmented into packet size messages and reassembled by the application
 *
 * does not work at method level
 *
 * WARNING: messages must not exceed packet size (default is 8kb, netted 7,8kB), as chaining requires ordered delivery
 * WARNING: this annotation does not inherit to subclasses automatically
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Unordered {
}
