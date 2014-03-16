package de.ruedigermoeller.fastcast.bigtest.services;

import de.ruedigermoeller.fastcast.remoting.FCFutureResultHandler;
import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.RemoteMethod;
import de.ruedigermoeller.fastcast.service.FCMembership;
import de.ruedigermoeller.fastcast.util.FCUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 2:10 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * holds a Hashtable and forwards all changes to ListenerNodes
 *
 */
public class HTHost extends FCTopicService {

    ConcurrentHashMap myData = new ConcurrentHashMap();
    int idFilter; // only record id's for which id%idFilter == 0

    HTListener remoteListeners;
    FCMembership localMembership;
    String hostNodeAddresses[];

    Executor listenerNot = FCUtils.createBoundedSingleThreadExecutor("listeners", 20000);

    @Override
    public void init() {
        remoteListeners = (HTListener) getRemoting().getRemoteService("htlisten");
        localMembership = (FCMembership) getRemoting().getService("membership");
    }

    @RemoteMethod(1)
    public void syncHostNumbers() {
        hostNodeAddresses = localMembership.getActiveNodeAdressesOrderDeterministic("HTHost");
        for (int i = 0; i < hostNodeAddresses.length; i++) {
            if ( hostNodeAddresses[i].equals(getNodeId()) ) {
                idFilter = i;
                System.out.println("set filter to "+i+" num hosts "+hostNodeAddresses.length);
            }
        }
    }

    @RemoteMethod(2)
    public void putData(final int id, final Object data) {
        if ( hostNodeAddresses == null || hostNodeAddresses.length == 0 )
            return;
        if ( (id%hostNodeAddresses.length) == idFilter ) {
            final Object old = myData.put(id, data);
            remoteListeners.receiveChange(id, old, data);
        }
    }


    /**
     * Caller probably needs to use extendTimeOut in order to get all entries !
     * @param res
     */
    @RemoteMethod(3)
    public void getTableData(final FCFutureResultHandler res) {
        new Thread("TableDataSender") {
            public void run() {
                for (Iterator<Map.Entry> iterator = myData.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry next = iterator.next();
                    res.sendResult(new Object[] { next.getKey(), next.getValue() });
                }
            }
        }.start();
    }

    @RemoteMethod(4)
    public void printTableSize() {
        System.out.println("table size"+myData.size());
    }

}
