/**
 * Copyright (c) 2014, Ruediger Moeller. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 *
 * Date: 03.01.14
 * Time: 21:19
 * To change this template use File | Settings | File Templates.
 */
package org.nustaq.fastcast.util;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/15/13
 * Time: 7:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class RateMeasure {

    int count;
    long lastStats;
    int checkEachMask = 127;
    long statInterval = 1000;
    long lastRatePersecond;

    String name = "none";

    public RateMeasure(String name, long statInterval) {
        this.name = name;
        this.statInterval = statInterval;
    }

    public RateMeasure(String name) {
        this.name = name;
    }

    public void count() {
        count++;
        if ( (count & ~checkEachMask) == count ) {
            checkStats();
        }
    }

    private void checkStats() {
        long now = System.currentTimeMillis();
        long diff = now-lastStats;
        if ( diff > statInterval ) {
            lastRatePersecond = count*1000l/diff;
            lastStats = now;
            count = 0;
            statsUpdated(lastRatePersecond);
        }
    }

    /**
     * override this
     * @param lastRatePersecond
     */
    protected void statsUpdated(long lastRatePersecond) {
        System.out.println("***** Stats for "+name+":   "+lastRatePersecond+"   per second *********");
    }


}
