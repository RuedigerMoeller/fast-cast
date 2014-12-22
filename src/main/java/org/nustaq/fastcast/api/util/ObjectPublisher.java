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
package org.nustaq.fastcast.api.util;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

/**
 * Created by ruedi on 13.12.14.
 */
public class ObjectPublisher {

    protected FCPublisher pub;
    protected FSTCoder coder;

    public ObjectPublisher(FCPublisher pub) {
        this.pub = pub;
        coder = new DefaultCoder();
    }

    public ObjectPublisher(FCPublisher pub, Class ... preregister) {
        this.pub = pub;
        coder = new DefaultCoder(preregister);
    }

    public void sendObject( String receiverNodeIdOrNull, Object toSend, boolean flush ) {
        byte[] bytes = coder.toByteArray(toSend);// fixme: performance. Need zerocopy variant in DefaultCoder
        while( ! pub.offer(receiverNodeIdOrNull,bytes,0,bytes.length,flush) ) {
            // spin
        }
    }

    public FCPublisher getPub() {
        return pub;
    }

    public ObjectPublisher batchOnLimit(boolean doBatch) {
        pub.batchOnLimit(doBatch);
        return this;
    }


}
