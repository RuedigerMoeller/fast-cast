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

import java.io.PrintStream;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/8/13
 * Time: 3:01 PM
 *
 * Loggerwrapper. Override out( int level, String msg, Throwable th) and use setInstance in order to redirect logging to
 * other loggers. Defaults to sysout
 *
 */
public class FCLog {

    public static final int DEBUG = 0;
    public static final int INFO = 1;
    public static final int WARN = 2;
    public static final int SEVER = 3;
    public static final int FATAL = 4;

    static FCLog instance;

    public static FCLog get() {
        if ( instance == null ) {
            instance = new FCLog();
        }
        return instance;
    }

    public static void setInstance(FCLog instance) {
        FCLog.instance = instance;
    }

    public static void log(String s) {
        get().info(s);
    }

    public static void log(String s, Throwable th) {
        get().info(s,th);
    }

    public static void log(Throwable th) {
        get().warn(th);
    }

    int logLevel = INFO;

    public int getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    void internal_out(int level, String msg, Throwable th) {
        out(level, msg, th);
    }

    protected void out(int level, String msg, Throwable th) {
        if ( level >= getLogLevel() ) {
            if ( msg != null )
                System.out.println(msg);
            if ( th != null )
                th.printStackTrace( new PrintStream(System.out));
        }
    }

    public void info( String msg ) {
        internal_out(INFO, msg, null);
    }

    public void info( String msg, Throwable e ) {
        internal_out(INFO, msg, e);
    }

    public void info( Throwable e ) {
        internal_out(INFO, null, e);
    }

    public void warn( String msg ) {
        internal_out(WARN, msg, null);
    }

    public void warn( String msg, Throwable e ) {
        internal_out(WARN, msg, e);
    }

    public void warn( Throwable e ) {
        internal_out(WARN, null, e);
    }

    public void severe( String msg, Throwable e ) {
        internal_out(SEVER, msg, e);
    }

    public void fatal( String msg) {
        internal_out(FATAL, msg, null);
    }

    public void fatal( String msg, Throwable e ) {
        internal_out(FATAL, msg, e);
    }

    public void debug( String msg, Throwable e ) {
        internal_out(DEBUG, msg, e);
    }

    public void debug( Throwable e ) {
        internal_out(DEBUG, null, e);
    }

    public void debug( String msg ) {
        internal_out(DEBUG, msg, null);
    }

    /**
     * called for RUDP low level logging (retransmission etc)
     * @param s
     */
    public void net(String s) {
        internal_out(INFO, s, null);
    }
}
