package de.ruedigermoeller.fastcast.packeting;

import de.ruedigermoeller.fastcast.config.FCTopicConf;
import de.ruedigermoeller.fastcast.control.FCTransportDispatcher;
import de.ruedigermoeller.fastcast.control.FlowControl;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.transport.Transport;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.fastcast.util.FCUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
* Created with IntelliJ IDEA.
* User: ruedi
* Date: 23.08.13
* Time: 02:04
* To change this template use File | Settings | File Templates.
*/
public class TopicEntry {

    FCTopicConf conf;
    FCTransportDispatcher channelDispatcher;
    String serviceClazz;
    FCTopicService service;
    FCRemoteServiceProxy serviceProxy;
    Method methods[];
    Class[][] methodArgs;
    FCCallbackMap cbMap;
    PacketSendBuffer sender;
    TopicStats stats;
    Transport trans;
    FlowControl control;
    ConcurrentHashMap<String,Long> senderHeartbeat = new ConcurrentHashMap<String, Long>();
    Executor replys;


    // loopback calls
    ExecutorService methodExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread res = new Thread(r);
            res.setName("Loopback calls");
            return res;
        }
    });
    boolean isUnordered = false;
    boolean isUnreliable = false;
    volatile boolean listenCalls = false; // if false => only listen to call results if any
    FCInvoker[] methodInvoker;
    private MsgReceiver msgReceiver;

    public TopicEntry(FCTopicConf conf) {
        this.conf = conf;
        if ( conf.getFlowControlClass() != null ) {
            try {
                control = (FlowControl) Class.forName(conf.getFlowControlClass()).newInstance();
            } catch (Exception e) {
                FCLog.log(e);  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        replys = FCUtils.createBoundedSingleThreadExecutor("reply-"+getConf().getName(),conf.getMaxOpenRespondedCalls());
    }

    public Executor getReplys() {
        return replys;
    }

    public boolean hasRemoteResultCalls() {
        if ( getServiceProxy() == null ) {
            throw new RuntimeException("no service class for this topic specified.");
        }
        return getServiceProxy().hasCallResultMethods();
    }

    public boolean isListenCalls() {
        return listenCalls;
    }

    public void setListenCalls(boolean listenCalls) {
        this.listenCalls = listenCalls;
    }

    public void registerHeartBeat(String sender, long time) {
        senderHeartbeat.put(sender,time);
    }

    public boolean hadHeartbeat(String sender) {
        return senderHeartbeat.containsKey(sender);
    }

    public List<String> getTimedOutSenders(long now, long timeout) {
        List<String> res = new ArrayList<String>();
        for (Iterator<String> iterator = senderHeartbeat.keySet().iterator(); iterator.hasNext(); ) {
            String next = iterator.next();
            long tim = senderHeartbeat.get(next);
            if ( now-tim > timeout ) {
                res.add(next);
            }
        }
        return res;
    }

    public FlowControl getControl() {
        return control;
    }

    public void setControl(FlowControl control) {
        this.control = control;
    }

    public FCTopicConf getConf() {
        return conf;
    }

    public Transport getTrans() {
        return trans;
    }

    public void setTrans(Transport trans) {
        this.trans = trans;
        stats = new TopicStats(trans.getConf().getDgramsize());
    }

    public ExecutorService getMethodExecutor() {
        return methodExecutor;
    }

    public void setConf(FCTopicConf conf) {
        this.conf = conf;
    }

    public FCCallbackMap getCbMap() {
        if ( cbMap == null ) {
            cbMap = new FCCallbackMap( conf.getMaxOpenRespondedCalls(), conf.getResponseMethodsTimeout());
        }
        return cbMap;
    }

    void setCbMap(FCCallbackMap cbMap) {
        this.cbMap = cbMap;
    }

    public boolean isUnordered() {
        return isUnordered;
    }

    public FCTransportDispatcher getChannelDispatcher() {
        return channelDispatcher;
    }

    public void setChannelDispatcher(FCTransportDispatcher channelDispatcher) {
        this.channelDispatcher = channelDispatcher;
    }

    public String getServiceClazz() {
        return conf.getServiceClass();
    }

    public FCTopicService getService() {
        return service;
    }

    public void setService(FCTopicService service) {
        isUnordered = service.getClass().getAnnotation(Unordered.class) != null;
        isUnreliable = service.getClass().getAnnotation(Unreliable.class) != null;
        this.service = service;
    }

    public boolean isUnreliable() {
        return isUnreliable;
    }

    public void setUnreliable(boolean unreliable) {
        isUnreliable = unreliable;
    }

    public FCRemoteServiceProxy getServiceProxy() {
        return serviceProxy;
    }

    public void setServiceProxy(FCRemoteServiceProxy serviceProxy) {
        this.serviceProxy = serviceProxy;
    }

    public Method[] getMethods() {
        return methods;
    }

    public void setSender(PacketSendBuffer sender) {
        this.sender = sender;
    }

    public Class[][] getMethodArgs() {
        return methodArgs;
    }

    public FCInvoker[] getMethodInvoker() {
        return methodInvoker;
    }

    public void setMethods(Method[] methods) {
        this.methods = new Method[128];
        methodArgs = new Class[this.methods.length][];
        methodInvoker = new FCInvoker[this.methods.length];
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            byte index = method.getAnnotation(RemoteMethod.class).value();
            if ( index > -1 ) {
                methodArgs[index] = method.getParameterTypes();
                this.methods[index] = method;
            }
        }
    }

    public PacketSendBuffer getSender() {
        return sender;
    }

    public int getTopic() {
        return conf.getTopic();
    }

    public TopicStats getStats() {
        if ( stats == null )
            stats = new TopicStats(((FastCast)FastCast.getRemoting()).getTransport(getConf().getTransport()).getConf().getDgramsize());
        return stats;
    }

    public void setMsgReceiver(MsgReceiver msgReceiver) {
        if ( this.msgReceiver != null ) {
            throw new RuntimeException("Only one msg recevier per topic allowed "+getConf().getName());
        }
        this.msgReceiver = msgReceiver;
    }

    public MsgReceiver getMsgReceiver() {
        return msgReceiver;
    }

    public void removeSenders(List<String> timedOutSenders) {
        for ( String s : timedOutSenders ) {
            senderHeartbeat.remove(s);
        }
    }

    public String getName() {
        return getConf().getName();
    }
}
