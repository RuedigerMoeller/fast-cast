package de.ruedigermoeller.fastcast.remoting;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * methods annotated with this will be called unsafe and out of order. Delivery is not guaranteed. Receive order is not
 * guaranteed. This can be useful for optional data or stuff like heartbeats. The advantage is, that in case of message loss
 * data transmission is not stalled until a retransmission arrives.
 *
 * Warning: message size cannot be larger than packet size (default is 16k) for unreliable messages. So do *not* use
 * large arguments.
 *
 * Warning: Unreliable Messages are not bundled. Each unreliable Message results in its own datagram.
 * WARNING: this annotation does not inherit to subclasses automatically
 *
 * This should not be a default mechanism, use rarely for special functionality.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Unreliable {
}
