package de.ruedigermoeller.fastcast.test;

import de.ruedigermoeller.fastcast.remoting.FCRemoting;
import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.remoting.RemoteMethod;

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
 * Time: 22:12
 * To change this template use File | Settings | File Templates.
 */

/**
 * receives ping and sends pong
 */
public class PongService extends FCTopicService {

    PingService ping;

    /**
     * override to do init and stuff
     */
    @Override
    public void init() {
        ping = (PingService) getRemoting().getRemoteService("ping");
    }

    @RemoteMethod(1)
    public void ping(long timeStamp) {
        if ( timeStamp % 1000 == 0 ) {
            System.out.println("Ping rec "+(System.nanoTime()-timeStamp)/1000000);
        }
        ping.pong(timeStamp, System.nanoTime());
    }

    public static void main( String arg[] ) throws IOException {
        FCRemoting fc = FastCast.getRemoting();
        fc.joinCluster("test/pingpong.yaml", "Pong", null);
        fc.startSending("ping");
        fc.startReceiving("pong");
    }

}
