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
package org.nustaq.fastcast.impl;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 15.08.13
 * Time: 01:30
 * 
 * Control packet to signal drops/heartbeats. These packets are always unreliable
 * ATTENTION: struct class
 */
public class ControlPacket extends Packet {
    public static final short DROPPED = 0;
    public static final short HEARTBEAT = 1; // NOT SENT AS CONTROL MESSAGE (required in stream to manage bootstrap)

    protected short type;

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }
}
