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

import org.nustaq.offheap.bytez.Bytez;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/11/13
 * Time: 1:07 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ByteArrayReceiver {
    public void receiveChunk(long sequence, Bytez b, int off, int len, boolean complete);
}
