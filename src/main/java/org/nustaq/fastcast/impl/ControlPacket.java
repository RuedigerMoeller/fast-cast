package org.nustaq.fastcast.impl;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 15.08.13
 * Time: 01:30
 * To change this template use File | Settings | File Templates.
 */
public class ControlPacket extends Packet {
    public static final short DROPPED = 0;
    public static final short HEARTBEAT = 99;

    protected short type;

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }
}
