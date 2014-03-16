package de.ruedigermoeller.fastcast.remoting;

import java.lang.annotation.*;

/**
 * defines that calls on a remote proxy for this method should be looped back to local instances.
 * Note that loopback calls are delivered directly to the local receiving queue. This should
 * have no impact on application logic.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Loopback {
}

