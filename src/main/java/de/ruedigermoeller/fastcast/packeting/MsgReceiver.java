package de.ruedigermoeller.fastcast.packeting;

import de.ruedigermoeller.heapoff.bytez.Bytez;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/12/13
 * Time: 3:10 PM
 * To change this template use File | Settings | File Templates.
 */
public interface MsgReceiver {

    public void messageReceived(String sender, long sequence, Bytez b, int off, int len);

}
