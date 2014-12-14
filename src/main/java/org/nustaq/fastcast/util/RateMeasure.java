/*
 * Copyright 2014 Ruediger Moeller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
