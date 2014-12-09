package org.nustaq.fastcast.transport;

import org.nustaq.fastcast.config.PhysicalTransportConf;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
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
 * Date: 25.05.13
 * Time: 20:21
 * To change this template use File | Settings | File Templates.
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
}
