package org.nustaq.fastcast.control;

import org.nustaq.fastcast.packeting.TopicStats;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 8/24/13
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class StupidFlowControl implements FlowControl {

    int statCount = 0;
    TopicStats fiveIntervals;
    TopicStats thirtyIntervals;

    public int adjustSendPause( int currentSendPause, TopicStats oneInterval) {
        statCount++;
        if ( fiveIntervals == null ) {
            fiveIntervals = new TopicStats(oneInterval.getDgramSize());
        }
        if ( thirtyIntervals == null ) {
            thirtyIntervals = new TopicStats(oneInterval.getDgramSize());
        }

        oneInterval.addTo(fiveIntervals,5);
        oneInterval.addTo(thirtyIntervals,30);

        double retransPerc = oneInterval.getRetransVSDataPacketPercentage();
        double retrans5Perc = fiveIntervals.getRetransVSDataPacketPercentage();
        if ( retransPerc > 1 ) {
            if ( retrans5Perc > .5 )
                return currentSendPause*3/2;
            else
                return currentSendPause*7/6;
        } else
        if ( retransPerc > .5 ) {
            if ( retrans5Perc > .3 )
                return currentSendPause*7/6;
            else
                return currentSendPause*13/12;
        } else
        if ( retransPerc > .2 ) {
            if ( retrans5Perc > .1)
                return currentSendPause*13/12;
            else {
                return currentSendPause*25/24;
            }
        } else
        if ( retransPerc < 0.1 ) {
            currentSendPause = currentSendPause*17/18;
        }
        if ( retrans5Perc < 0.1 ) {
            return Math.min(currentSendPause*17/18, currentSendPause-1);
        }
        return currentSendPause;
    }

    public int getStatCount() {
        return statCount;
    }

    public TopicStats getFiveIntervals() {
        return fiveIntervals;
    }

    public TopicStats getThirtyIntervals() {
        return thirtyIntervals;
    }
}
