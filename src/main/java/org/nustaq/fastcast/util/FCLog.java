package org.nustaq.fastcast.util;

import org.nustaq.fastcast.remoting.FastCast;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

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
    public static final int CLUSTER = 5;
    public static final int CLUSTER_LISTENER = 6;

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

    /**
     * used by listening node for clusterwide logging, don't use directly
     * @param msg
     */
    public void internal_clusterListenerLog(String msg) {
        internal_out(CLUSTER_LISTENER, msg, null);
    }

    /**
     * log to cluster wide log (requires FCMemberShip topic service installed)
     * @param msg
     */
    public void cluster(String msg) {
        internal_out(CLUSTER, msg, null);
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
