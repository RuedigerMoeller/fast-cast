package de.ruedigermoeller.fastcast.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
 * <p/>
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * <p/>
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 * <p/>
 * Date: 30.01.13
 * Time: 20:19
 * To change this template use File | Settings | File Templates.
 */
public class FCUtils {

    public static boolean NETLOG = true;
    public static boolean FAT_NODE_NAME = false; // nodeid includes host if true

    public static class FCIncomingMessageThread extends Thread {
        public FCIncomingMessageThread() {
            super();
        }

        public FCIncomingMessageThread(Runnable target) {
            super(target);
        }

        public FCIncomingMessageThread(String name) {
            super(name);
        }

        public FCIncomingMessageThread(Runnable target, String name) {
            super(target, name);
        }
    }

    public static Executor createIncomingMessageThreadExecutor( final String name, int qsize ) {
        ThreadPoolExecutor res = new ThreadPoolExecutor(1,1,1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(qsize));
        res.setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new FCIncomingMessageThread( r, name );
            }
        });
        res.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                while ( !executor.isShutdown() && !executor.getQueue().offer(r)) {
                    Thread.yield();
                }
            }
        } );
        return res;
    }

    public static Executor createBoundedSingleThreadExecutor( final String name, int qsize ) {
        ThreadPoolExecutor res = new ThreadPoolExecutor(1,1,1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(qsize));
        res.setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread( r, name );
            }
        });
        res.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                while ( !executor.isShutdown() && !executor.getQueue().offer(r)) {
                    Thread.yield();
                }
            }
        } );
        return res;
    }

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
