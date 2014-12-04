package org.nustaq.fastcast.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;

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
 * Date: 30.01.13
 * Time: 20:19
 * To change this template use File | Settings | File Templates.
 */
public class FCUtils {

    public static boolean FAT_NODE_NAME = false; // nodeid includes host if true

    public static String createNodeId( String addendum ) {
        if ( FAT_NODE_NAME ) {
            String host = "unknown";
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                FCLog.get().severe(null,e);
            }
            return host+"-"+addendum+"-"+(int)(Math.random()*1000000);
        } else {
            return addendum+"-"+Integer.toHexString((int)(Math.random()*1024000));
        }
    }

    public static boolean isWindows() {
        return System.getProperty("os.name","").indexOf("indows") > 0;
    }
}
