package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.heapoff.bytez.Bytez;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/12/13
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCBinaryTopicService extends FCTopicService {

    FCBinaryMessageListener listener;

    /**
     * empty constructor required for bytecode magic
     */
    public FCBinaryTopicService() {
    }

    public FCBinaryTopicService(FCBinaryMessageListener listener) {
        this.listener = listener;
    }

    @Override
    @RemoteMethod(-1)
    public void receiveBinary(Bytez bytes, int offset, int length) {
        listener.receiveBinary(bytes,offset,length);
    }
}
