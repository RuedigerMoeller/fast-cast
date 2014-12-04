package de.ruedigermoeller.fastcast.test;

import org.nustaq.fastcast.api.FCRemoting;
import org.nustaq.fastcast.api.FCTopicService;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.api.RemoteMethod;

import java.io.IOException;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
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
 * Date: 01.09.13
 * Time: 21:30
 * To change this template use File | Settings | File Templates.
 */
public class PingService extends FCTopicService {

    @RemoteMethod(1)
    public void pong(long sendTime, long receiveTime) {
        long pongRec = System.nanoTime();
        if ( pongRec % 1000 == 0 ) {
            System.out.println("Ping Pong send "+(receiveTime-sendTime)/1000+" round "+(pongRec-sendTime)/1000);
        }
    }

    public static void main( String arg[] ) throws IOException {
        FCRemoting fc = FastCast.getFastCast();
        fc.joinCluster("test/pingpong.yaml", "Ping", null);

        fc.startReceiving("ping");
        fc.startSending("pong");

        final PongService pong = (PongService) fc.getRemoteService("pong");

        new Thread() {
            public void run() {
                while(true) {
                    long timeStamp = System.nanoTime();
                    pong.ping(timeStamp);
                }
            }
        }.start();
    }
}
