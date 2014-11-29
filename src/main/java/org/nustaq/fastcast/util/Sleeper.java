package org.nustaq.fastcast.util;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/12/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class Sleeper {

    int sleepcount = 0;
    public void sleepMicros(int micro) {
        if ( micro <= 0 )
            return;
        sleepcount+=micro;
        try {
            if ( sleepcount >= 1000 ) {
                Thread.sleep(sleepcount/1000);
                sleepcount%=1000;
            }
        } catch (InterruptedException e) {
        }
    }

}
