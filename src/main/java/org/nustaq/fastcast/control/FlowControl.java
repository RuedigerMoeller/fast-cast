package org.nustaq.fastcast.control;

import org.nustaq.fastcast.packeting.TopicStats;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/25/13
 * Time: 1:49 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FlowControl {
    public int adjustSendPause( int currentSendPause, TopicStats oneInterval);

}
