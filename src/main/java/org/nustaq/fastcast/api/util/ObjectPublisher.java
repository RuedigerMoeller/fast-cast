/*
 * Copyright 2014 Ruediger Moeller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
