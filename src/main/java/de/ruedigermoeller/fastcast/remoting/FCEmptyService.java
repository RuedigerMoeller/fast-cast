package de.ruedigermoeller.fastcast.remoting;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/5/13
 * Time: 5:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class FCEmptyService extends FCTopicService {

    @RemoteMethod(1) @Loopback
    public void message(String message) {
        System.out.println("received message:"+message );
    }

}
