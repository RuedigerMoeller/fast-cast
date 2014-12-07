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
