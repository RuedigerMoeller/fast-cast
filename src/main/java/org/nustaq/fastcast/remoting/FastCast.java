package org.nustaq.fastcast.remoting;

import org.nustaq.fastcast.config.FCPublisherConf;
import org.nustaq.fastcast.config.FCSocketConf;
import org.nustaq.fastcast.config.FCSubscriberConf;
import org.nustaq.fastcast.control.FCTransportDispatcher;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.fastcast.util.FCUtils;
import org.nustaq.fastcast.packeting.*;
import org.nustaq.fastcast.transport.*;
import org.nustaq.offheap.structs.unsafeimpl.FSTStructFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/6/13
 * Time: 3:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class FastCast {

    public static final int HEARTBEAT = 99;

    static {
        FSTStructFactory.getInstance().registerSystemClz((byte)127, Packet.class, DataPacket.class, RetransPacket.class, RetransEntry.class, ControlPacket.class);
    }

    static FastCast fc;
    private String clusterName = "-";

    public static FastCast getFastCast() {
        synchronized (FastCast.class) {
            if ( fc != null ) {
                return fc;
            }
            fc = new FastCast();
            FCLog.get().internal_clusterListenerLog(
                    "____ ____ ____ ___ ____ ____ ____ ___\n" +
                            "|--- |--| ====  |  |___ |--| ====  |  \n" + "> v2"
            );
            fc.setNodeId("N"+(int)(Math.random()*10000));
            return fc;
        }
    }

    protected HashMap<String,Transport> transports = new HashMap<String, Transport>();
    protected HashMap<String,FCTransportDispatcher> dispatcher = new HashMap<String, FCTransportDispatcher>();
    protected HashMap<Integer,TopicEntry> topics = new HashMap<Integer, TopicEntry>();
    String nodeId;

    public void setNodeId(String nodeName) {
        if ( nodeId != null )
            throw new RuntimeException("Node Id can only be set ponce per process");
        nodeId = FCUtils.createNodeId(nodeName);
    }

    public String getNodeId() {
        return nodeId;
    }

    public Transport getTransport(String name) {
        Transport transport = transports.get(name);
        if ( transport == null ) {
            FCLog.log("could not find transport '" + name + "'. Falling back to transport 'default'");
            return transports.get("default");
        }
        return transport;
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // impl
    //

    TopicEntry getTopic(String name) {
        return topics.get(name);
    }

    public TopicStats getStats(String name) {
        TopicEntry topic = getTopic(name);
        if ( topic != null )
            return topic.getStats();
        return null;
    }

    public List<Integer> getActiveTopics() {
        ArrayList<Integer> res = new ArrayList<>();
        for (Iterator<TopicEntry> iterator = topics.values().iterator(); iterator.hasNext(); ) {
            TopicEntry next = iterator.next();
            if ( next.getSender() != null ) {
                res.add(next.getReceiverConf().getTopicId());
            }
        }
        Collections.sort(res);
        return res;
    }

//    @Override
//    public void startReceiving(String topicName, FCTopicService service) {
//        TopicEntry topic = getTopicId(topicName);
//        topic.setService(service);
//        topic.getConf().setServiceClass(service.getClass().getName());
//        startReceiving(topicName);
//    }

    // removed from public interface, confusion on how to init
//    void startReceiving(String topicName) {
//        TopicEntry topic = getTopicId(topicName);
//        try {
//            if ( topic.getService() == null ) {
//                topic.setService((FCTopicService) Class.forName(topic.getServiceClazz()).newInstance());
//            }
//            initServiceClz(topic, topic.getServiceClazz());
//        } catch (Exception e) {
//            throw FSTUtil.rethrow(e);
//        }
//        if ( topic.hasRemoteResultCalls() && ! topic.getChannelDispatcher().hasSender(topic) ) {
//            startSending(topicName);
//        }
//        topic.getService().initializeBeforeListening(this, nodeId, topic.getConf().getName(), topic.getTopicId());
//        if ( FCMembership.class.getName().equals(topic.getServiceClazz()) ) {
//            memberShipLocal = (FCMembership) getService(topic.getName());
//        }
//        topic.getService().init();
//        if ( topic.getChannelDispatcher().hasReceiver(topic) ) {
//            // already limited listener present
//            topic.setListenCalls(true);
//        } else {
//            topic.getChannelDispatcher().startListening(topic);
//            topic.setListenCalls(true);
//        }
//    }

//    @Override
//    public void start(String topicName) {
//        startSending(topicName);
//        startReceiving(topicName);
//    }
//
//    public <T extends FCTopicService> T startSending(String topic, Class<T> fcBinaryTopicServiceClass) throws Exception {
//        TopicEntry te = getTopicId(topic);
//        // fixme: remove double values in conf and topicentry
//        te.getConf().setServiceClass(fcBinaryTopicServiceClass.getName());
//        installService(te);
//        startSending(topic);
//        return (T) te.getServiceProxy();
//    }
//
//    FCRemoteServiceProxy startSending(String topicName) {
//        TopicEntry topic = getTopicId(topicName);
//        FCTransportDispatcher dispatcher = topic.getChannelDispatcher();
//        dispatcher.installSender(topic);
//        dispatcher.putHeartbeat(topic.getSender());
//        if ( topic.hasRemoteResultCalls() ) {
//            // need to add a limited receiver if not present
//            if ( ! dispatcher.hasReceiver(topic) ) {
//                topic.setListenCalls(false);
//                topic.getChannelDispatcher().startListening(topic);
//            }
////            try {
////                Thread.sleep((long) (topic.getConf().getHeartbeatInterval()*2));
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//        }
//        if ( FCMembership.class.getName().equals(topic.getServiceClazz()) ) {
//            memberShipRemote = (FCMembership) topic.getServiceProxy();
//        }
//        return topic.getServiceProxy();
//    }

//    @Override
//    public void stopReceiving(String topicName) {
//        TopicEntry topic = getTopicId(topicName);
//        topic.getChannelDispatcher().stopListening(topic);
//    }
//
//    @Override
//    public FCRemoteServiceProxy getRemoteService(String topic) {
//        TopicEntry topicEntry = topics.get(topic);
//        if ( topicEntry == null ) {
//            return null;
//        }
//        return topicEntry.getServiceProxy();
//    }

    public FCTransportDispatcher getTransportDispatcher( String transName ) {
        FCTransportDispatcher res = dispatcher.get(transName);
        if ( res == null ) {
            Transport transport = getTransport(transName);
            res = new FCTransportDispatcher( transport, clusterName, nodeId );
            dispatcher.put(transName,res);
        }
        return res;
    }

    public void subscribe( FCSubscriberConf subsConf, FCSubscriber subscriber ) {
        TopicEntry topicEntry = topics.get(subsConf.getTopicId());
        if ( topicEntry == null )
            topicEntry = new TopicEntry(subsConf,null);
        if ( topicEntry.getSubscriber() != null) {
            throw new RuntimeException("already subscribed on "+subsConf.getTopicId());
        }
        FCTransportDispatcher dispatcher = getTransportDispatcher(subsConf.getTransport());
        topicEntry.setReceiverConf(subsConf);
        topicEntry.setChannelDispatcher(dispatcher);
        topicEntry.setSubscriber(subscriber);
        topics.put(subsConf.getTopicId(), topicEntry);
        dispatcher.installReceiver(topicEntry,subscriber);
    }

    public FCPublisher publish( FCPublisherConf pubConf ) {
        TopicEntry topicEntry = topics.get(pubConf.getTopicId());
        if ( topicEntry == null )
            topicEntry = new TopicEntry(null,null);
        if ( topicEntry.getPublisherConf() != null ) {
            throw new RuntimeException("already a sender registered at "+pubConf.getTopicId());
        }
        FCTransportDispatcher dispatcher = getTransportDispatcher(pubConf.getTransport());
        topicEntry.setChannelDispatcher(dispatcher);
        topicEntry.setPublisherConf(pubConf);
        topics.put(pubConf.getTopicId(), topicEntry);
        final PacketSendBuffer packetSendBuffer = dispatcher.installSender(topicEntry);
        return packetSendBuffer;
    }

//    private MsgReceiver createMsgReceiver(final TopicEntry topic) {
//        // CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED
//        return new MsgReceiver() {
//            @Override
//            public void messageReceived(final String sender, long sequence, Bytez bz, int off, int len) {
//                ReceiverThreadContext ct = recCtx.get();
//                boolean listenMethods = topic.isListenCalls();
//                // major dispatch of remote methods
//                try {
//                    if ( listenMethods && len > 0 && bz.get(off) == BINARY ) {
//                        topic.getService().receiveBinary(bz,off+1,len-1);
//                        return;
//                    }
//
//                    prepareReceiveContext(sender, topic);
//
//                    int code = bz.get(off);
//
//                    if (listenMethods && (code == REMOTE_CALL || code == FAST_CALL) ) {
//                        FCTopicService callTarget = topic.getService();
//                        int methodIndex = bz.get(off+1);
//
//                        int flowHeader = callTarget.readAndFilter(methodIndex, bz, off + 2);
//                        if ( flowHeader < 0 ) {
//                            return;
//                        }
//
//                        byte b[] = bz.toBytes(off,len);
//                        FSTObjectInput fstObjectInput = getNewFstObjectInput(b, 0, len);
//
//                        // skip 2 bytes+flow header. read/write asymetry
//                        for (int i=0; i < flowHeader+2; i++)
//                            fstObjectInput.readByte();
//
//                        byte isRec = fstObjectInput.readByte();
//                        if ( isRec == 1 ) {
//                            String rec = fstObjectInput.readStringUTF();
//                            if ( ! rec.equals(getNodeId()) )
//                                return;
//                        }
//
//                        Object args[] = ct.zeroArgs;
//                        int numArgs = fstObjectInput.readByte();
//                        final Method m = topic.getMethods()[methodIndex];
//                        Class[] argTypes = topic.getMethodArgs()[methodIndex];
//
//                        boolean hasRemoteResult = argTypes != null && argTypes.length > 0 && argTypes[argTypes.length - 1] == FCFutureResultHandler.class;
//                        if ( numArgs > 0 ) {
//
//                            // prepare/reuse arg array
//                            if (!hasRemoteResult) {
//                                switch (numArgs) {
//                                    case 1: args = ct.oneArgs; break;
//                                    case 2: args = ct.twoArgs; break;
//                                    case 3: args = ct.threeArgs; break;
//                                    case 4: args = ct.fourArgs; break;
//                                    default:
//                                        args = new Object[numArgs];
//                                }
//                            } else {
//                                args = new Object[numArgs];
//                            }
//
//                            // give FCTopicService a shot to track/consume calls
//                            boolean invoked = callTarget.invoke(methodIndex, m, fstObjectInput,argTypes);
//                            if ( invoked ) {
//                                return;
//                            }
//
//                            FCInvoker fcInvoker = topic.getMethodInvoker()[methodIndex];
//                            if ( fcInvoker == null ) {
//                                fcInvoker = topic.getMethodInvoker()[methodIndex] = proxyFactory.getMethod(topic.getServiceClazz(),methodIndex);
//                            }
//                            if ( fcInvoker != null ) {
//                                fcInvoker.invoke(callTarget,fstObjectInput);
//                                // invokers are only avaiable if no remote result is avaiable,
//                                // safely return
//                                return;
//                            }
//
//                            // decode args
//                            switch (code) {
//                                case REMOTE_CALL:
//                                    for ( int i = 0; i < numArgs; i++ ) {
//                                        args[i] = fstObjectInput.readObject();
//                                    }
//                                    break;
//                                case FAST_CALL:
//                                    decodeFastCall(fstObjectInput, argTypes, args);
//                                    break;
//                            }
//                        }
//                        if (hasRemoteResult) {
//                            // replace last argument by result dispatcher
//                            final Long cbId = (Long) args[args.length - 1];
//                            args[args.length - 1] = createRemoteResultDispatcher(sender, cbId, topic);
//                        }
//                        try {
//                            // slow reflection call
//                            m.invoke(callTarget, args);
//                        } catch (Exception e) {
//                            FCLog.log(e);
//                        }
//
//                    } else if (code == CALL_RESULT) {
//                        byte b[] = bz.toBytes(off+1,len-1);
//                        FSTObjectInput fstObjectInput = getNewFstObjectInput(b, 0, len);
//                        String receiver = fstObjectInput.readStringUTF();
////                        System.out.println("CB to "+receiver+" my nodid "+nodeId);
//                        if ( nodeId.equals(receiver) ) {
//                            long cbid = fstObjectInput.readLong();
////                            System.out.println("  cbid "+cbid);
//                            Object result = fstObjectInput.readObject();
//                            FCFutureResultHandler fcFutureResultHandler = topic.getCbMap().get(cbid);
//                            if (fcFutureResultHandler != null) {
////                                System.out.println("hit cb 1 "+cbid);
//                                fcFutureResultHandler.resultReceived(result, sender);
//                            } else {
////                                System.out.println("missed cb 1 "+cbid);
//                            }
//                        } else {
////                            System.out.println("  omitted cbid "+fstObjectInput.readFLong());
//                        }
//                    } else if (code == HEARTBEAT) {
////                        System.out.println("heartbeat from "+sender);
////                        if ( listenMethods )
//                        {
//                            long now = System.currentTimeMillis();
//                            topic.registerHeartBeat(sender, now);
//                            if ( now-ct.lastSenderCleanUp > topic.getConf().getSenderTimeoutMillis() ) {
//                                List<String> timedOutSenders = topic.getTimedOutSenders(now, 30000);
//                                topic.removeSenders(timedOutSenders);
//                                topic.getChannelDispatcher().cleanup(timedOutSenders, topic.getTopicId() );
//                                ct.lastSenderCleanUp = now;
//                            }
//                        }
//                    } else {
//                        if ( listenMethods )
//                            FCLog.get().severe("unknown code " + code, new Exception("stack trace"));
//                    }
//                } catch (Exception e) {
//                    FCLog.log(e);
//                }
//            }
//        };
//    }

    private void prepareReceiveContext(String sender, TopicEntry topic) {
        FCReceiveContext fcReceiveContext = FCReceiveContext.get();
        fcReceiveContext.sender = sender;
        fcReceiveContext.entry = topic;
    }

    protected void initTransports(FCSocketConf[] tconfs, String nodeName) {
        FCLog.log("connecting transports as '"+nodeName+"' in cluster:'"+clusterName+"'");
        for (int i = 0; i < tconfs.length; i++) {
            FCSocketConf tconf = tconfs[i];
            createTransport(tconf);
        }
    }

    public static class ConfigurationAlreadyDefinedException extends RuntimeException {
        public ConfigurationAlreadyDefinedException(String message) {
            super(message);
        }
    }

    /**
     * mother of all init*Transport methods ..
     *
     * @param tconf
     */
    public void createTransport(FCSocketConf tconf) {
        if ( nodeId == null )
            throw new RuntimeException("define nodeId first");
        if ( transports.get(tconf.getName()) != null ) {
            throw new ConfigurationAlreadyDefinedException("transport "+tconf.getName()+" already initialized ");
        }
        try {
            FCLog.log("Connecting transport " + tconf.getName());
            // fixme: use Guice
            if ( FCSocketConf.MCAST_NIO_SOCKET.equals(tconf.getTransportType()) ) {
                FCMulticastChannelTransport tr = new FCMulticastChannelTransport(tconf);
                tr.join();
                transports.put(tconf.getName(), tr);
            } else if (FCSocketConf.MCAST_SOCKET.equals(tconf.getTransportType())) {
                FCMulticastSocketTransport tr = new FCMulticastSocketTransport(tconf);
                tr.join();
                transports.put(tconf.getName(), tr);
            } else if (FCSocketConf.MCAST_IPC.equals(tconf.getTransportType())) {
                SharedMemTransport tr = new SharedMemTransport(tconf);
                tr.join();
                transports.put(tconf.getName(), tr);
            } else {
                throw new RuntimeException("unknown transport "+tconf.getTransportType());
            }
        } catch (IOException e) {
            FCLog.log(e);
        }
    }

}
