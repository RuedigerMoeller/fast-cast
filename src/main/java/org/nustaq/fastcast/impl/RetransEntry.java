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
package org.nustaq.fastcast.impl;

import org.nustaq.offheap.structs.FSTStruct;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/12/13
 * Time: 8:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class RetransEntry extends FSTStruct {
    protected long from;
    protected long to;

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "RetransEntry{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }

    public long getNumPackets() {
        return to-from;
    }
}
