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
package org.nustaq.fastcast.transport;

import org.nustaq.fastcast.config.PhysicalTransportConf;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

/**
 */
public interface PhysicalTransport {

    public boolean receive(ByteBuffer pack) throws IOException;
    public boolean receive(DatagramPacket pack) throws IOException;
    public void send(DatagramPacket pack) throws IOException;
    public void send(byte bytes[], int off, int len) throws IOException;
    public void send(ByteBuffer b) throws IOException;
    public void join() throws IOException;
    public PhysicalTransportConf getConf();

    void close();

    boolean isBlocking();
}
