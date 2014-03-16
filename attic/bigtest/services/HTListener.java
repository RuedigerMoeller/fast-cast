package de.ruedigermoeller.fastcast.bigtest.services;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 2:11 PM
 * To change this template use File | Settings | File Templates.
 */

import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.RemoteMethod;

import java.util.concurrent.ConcurrentHashMap;

/**
 * listenes to HT changes. Note: configure as threadPerSender
 */
public class HTListener extends FCTopicService {

    ConcurrentHashMap mirror = new ConcurrentHashMap();
    DataListener listener;

    public DataListener getListener() {
        return listener;
    }

    public void setListener(DataListener listener) {
        this.listener = listener;
    }

    public static interface DataListener {
        public void changeReceived(int id, Object prev, Object newVal);
    }

    @RemoteMethod(1)
    public void receiveChange( int id, Object previousValue, Object newValue) {
        mirror.put(id,newValue);
        if ( listener != null ) {
            listener.changeReceived(id,previousValue,newValue);
        }
    }

    public ConcurrentHashMap getMirror() {
        return mirror;
    }
}
