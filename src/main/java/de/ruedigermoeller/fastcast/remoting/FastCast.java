package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.config.FCLocalClusterConf;
import de.ruedigermoeller.fastcast.config.FCTopicConf;
import de.ruedigermoeller.fastcast.packeting.*;
import de.ruedigermoeller.fastcast.service.FCMembership;
import de.ruedigermoeller.fastcast.transport.*;
import de.ruedigermoeller.fastcast.control.FCTransportDispatcher;
import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.fastcast.util.FCUtils;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;
import de.ruedigermoeller.heapoff.structs.unsafeimpl.FSTStructFactory;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import de.ruedigermoeller.serialization.util.FSTUtil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/6/13
 * Time: 3:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class FastCast implements FSTObjectInput.ConditionalCallback, FCRemoting {

    static final int BINARY = 55;
    static final int FAST_CALL = 66;
    static final int REMOTE_CALL = 77;
    static final int CALL_RESULT = 88;
    public static final int HEARTBEAT = 99;

    private final FCProxyFactory proxyFactory = new FCProxyFactory();

    protected FCClusterConfig config;
    protected static FSTConfiguration conf;
    static {
        System.setProperty("fst.unsafe", "false");
        conf = FSTConfiguration.createDefaultConfiguration();
        conf.setPreferSpeed(true);
        FSTStructFactory.getInstance().registerSystemClz((byte)127, Packet.class, DataPacket.class, RetransPacket.class, RetransEntry.class, ControlPacket.class);
    }

    public static FSTConfiguration getSerializationConfig() {
        return conf;
    }

    static FastCast fc;
    public static FCRemoting getRemoting() {
        synchronized (FCRemoting.class) {
            if ( fc != null ) {
                return fc;
            }
            fc = new FastCast();
            FCLog.get().internal_clusterListenerLog(
                    "____ ____ ____ ___ ____ ____ ____ ___\n" +
                            "|--- |--| ====  |  |___ |--| ====  |  \n" + "> v2"
            );


            return fc;
        }
    }

    protected HashMap<String,Transport> transports = new HashMap<String, Transport>();
    protected HashMap<String,FCTransportDispatcher> dispatcher = new HashMap<String, FCTransportDispatcher>();
    protected HashMap<String,FCTopicConf> channelConf = new HashMap<String, FCTopicConf>();
    protected HashMap<String,TopicEntry> topics = new HashMap<String, TopicEntry>();
    String nodeId;
    FCRemotingListener listener;

    public void joinCluster(String configFile, String nodeId, String clusterName) throws IOException {
        FCClusterConfig conf = FCClusterConfig.read(configFile);
        String over = new File(configFile).getAbsoluteFile().getParentFile().getParent()+File.separator+"local"+File.separator+new File(configFile).getName();
        if ( ! new File(over).exists() ) {
            over = "."+File.separator+"local"+File.separator+configFile;
        }
        if ( new File(over).exists() ) {
            FCLocalClusterConf local = FCLocalClusterConf.read(over);
            conf.overrideBy(local);
        }
        joinCluster(conf, nodeId, clusterName);
    }

    public FCRemotingListener getRemotingListener() {
        return listener;
    }

    public void setRemotingListener(FCRemotingListener listener) {
        this.listener = listener;
    }

    public void joinCluster(FCClusterConfig conf, String nodeId, String clusterName) {
        if ( clusterName != null && clusterName.length() > 0 )
            conf.setClusterName(clusterName);
        start(conf , nodeId);
    }

    public void start( FCClusterConfig config, String nodeName) {
        this.config = config;
        setNodeId(nodeName);
        FCLog.get().setLogLevel(config.getLogLevel());
        initTransports(config.getTransports(), nodeName);
        FCTopicConf[] topicsList = config.getTopics();
        initTopics(topicsList);

//        long hbmax = 0;
//        for (int i = 0; i < topicsList.length; i++) {
//            FCTopicConf fcTopicConf = topicsList[i];
//            hbmax = Math.max(fcTopicConf.getHeartbeatInterval(), hbmax);
//        }
//
//        if ( hbmax > 0 ) {
//            try {
//                Thread.sleep(hbmax);
//            } catch (InterruptedException e) {
//            }
//        }
    }

    public void setNodeId(String nodeName) {
        if ( nodeId != null )
            throw new RuntimeException("Node Id can only be set ponce per process");
        nodeId = FCUtils.createNodeId(nodeName);
    }

    public FCRemoteServiceProxy getServiceProxy(String topic) {
        TopicEntry topicEntry = topics.get(topic);
        if ( topicEntry == null )
            return null;
        return topicEntry.getServiceProxy();
    }

    public String getNodeId() {
        return nodeId;
    }

    public FCTopicService getService(String topic) {
        TopicEntry topicEntry = topics.get(topic);
        if ( topicEntry == null ) {
            return null;
        }
        return topicEntry.getService();
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

    public List<String> getActiveTopics() {
        ArrayList<String> res = new ArrayList<>();
        for (Iterator<TopicEntry> iterator = topics.values().iterator(); iterator.hasNext(); ) {
            TopicEntry next = iterator.next();
            if ( next.isListenCalls() || next.getSender() != null ) {
                res.add(next.getConf().getName());
            }
        }
        Collections.sort(res);
        return res;
    }

    public FCTopicConf getTopicConfiguration(String name) {
        return getTopic(name).getConf();
    }

    @Override
    public void startReceiving(String topicName, FCBinaryMessageListener binaryListener) {
        startReceiving(topicName, new FCBinaryTopicService(binaryListener));
    }

    @Override
    public void startReceiving(String topicName, FCTopicService service) {
        TopicEntry topic = getTopic(topicName);
        topic.setService(service);
        topic.getConf().setServiceClass(service.getClass().getName());
        startReceiving(topicName);
    }

    // removed from public interface, confusion on how to init
    void startReceiving(String topicName) {
        TopicEntry topic = getTopic(topicName);
        try {
            if ( topic.getService() == null ) {
                topic.setService((FCTopicService) Class.forName(topic.getServiceClazz()).newInstance());
            }
            initServiceClz(topic, topic.getServiceClazz());
        } catch (Exception e) {
            throw FSTUtil.rethrow(e);
        }
        if ( topic.hasRemoteResultCalls() && ! topic.getChannelDispatcher().hasSender(topic) ) {
            startSending(topicName);
        }
        topic.getService().initializeBeforeListening(this, nodeId, topic.getConf().getName(), topic.getTopic());
        if ( FCMembership.class.getName().equals(topic.getServiceClazz()) ) {
            memberShipLocal = (FCMembership) getService(topic.getName());
        }
        topic.getService().init();
        if ( topic.getChannelDispatcher().hasReceiver(topic) ) {
            // already limited listener present
            topic.setListenCalls(true);
        } else {
            topic.getChannelDispatcher().startListening(topic);
            topic.setListenCalls(true);
        }
    }

    @Override
    public void start(String topicName) {
        startSending(topicName);
        startReceiving(topicName);
    }

    public <T extends FCTopicService> T startSending(String topic, Class<T> fcBinaryTopicServiceClass) throws Exception {
        TopicEntry te = getTopic(topic);
        // fixme: remove double values in conf and topicentry
        te.getConf().setServiceClass(fcBinaryTopicServiceClass.getName());
        installService(te);
        startSending(topic);
        return (T) te.getServiceProxy();
    }

    FCRemoteServiceProxy startSending(String topicName) {
        TopicEntry topic = getTopic(topicName);
        FCTransportDispatcher dispatcher = topic.getChannelDispatcher();
        dispatcher.installSender(topic);
        dispatcher.putHeartbeat(topic.getSender());
        if ( topic.hasRemoteResultCalls() ) {
            // need to add a limited receiver if not present
            if ( ! dispatcher.hasReceiver(topic) ) {
                topic.setListenCalls(false);
                topic.getChannelDispatcher().startListening(topic);
            }
//            try {
//                Thread.sleep((long) (topic.getConf().getHeartbeatInterval()*2));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
        if ( FCMembership.class.getName().equals(topic.getServiceClazz()) ) {
            memberShipRemote = (FCMembership) topic.getServiceProxy();
        }
        return topic.getServiceProxy();
    }

    @Override
    public void stopReceiving(String topicName) {
        TopicEntry topic = getTopic(topicName);
        topic.getChannelDispatcher().stopListening(topic);
    }

    @Override
    public FCRemoteServiceProxy getRemoteService(String topic) {
        TopicEntry topicEntry = topics.get(topic);
        if ( topicEntry == null ) {
            return null;
        }
        return topicEntry.getServiceProxy();
    }

    public boolean hasRemoteResult(Object args[]) {
        return args != null && args.length > 0 && args[args.length-1] instanceof FCFutureResultHandler;
    }

    ThreadLocal<FSTObjectOutput> out = new ThreadLocal<FSTObjectOutput>();
    ThreadLocal<FSTObjectInput> in = new ThreadLocal<FSTObjectInput>();

    public FSTObjectOutput prepareFastCall( int methodIndex, int args) throws IOException {
        FSTObjectOutput fstObjectOutput = out.get();
        if ( fstObjectOutput == null ) {
            fstObjectOutput = new FSTObjectOutput(conf);
            out.set(fstObjectOutput);
        }
        fstObjectOutput.resetForReUse();
        fstObjectOutput.writeFByte(FAST_CALL);
        fstObjectOutput.writeFByte(methodIndex);

        byte flowFilter[] = FCSendContext.get().getFlowHeader();
        if ( flowFilter != null )
            fstObjectOutput.writeFByteArr(flowFilter);

        if ( FCSendContext.get().getReceiver() == null )
            fstObjectOutput.writeFByte(0);
        else {
            fstObjectOutput.writeFByte(1);
            fstObjectOutput.writeStringUTF(FCSendContext.get().getReceiver());
        }
        fstObjectOutput.writeFByte(args);
        return fstObjectOutput;
    }

    public void finishFastCall(final PacketSendBuffer sender, final FSTObjectOutput fstObjectOutput) {
        final byte[] buffer = fstObjectOutput.getBuffer();
        final int written = fstObjectOutput.getWritten();
        finishFastCallImpl(sender, new HeapBytez(buffer),written);
    }

    private void finishFastCallImpl(PacketSendBuffer sender, Bytez buffer, int written) {
        //FIXME: loopback + unreliable + avoid new byte[]
        sender.putMessage(-1, buffer, 0, written, false);
        FCSendContext.get().reset();
    }

    public void sendBinaryContent( final TopicEntry topicEntry, PacketSendBuffer sender, final Bytez bytes, final int offset, final int length, boolean loopback ) throws IOException {
        if ( loopback ) {
            topicEntry.getMethodExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    prepareReceiveContext(nodeId, topicEntry);
                    topicEntry.getService().receiveBinary( bytes, offset, length );
                }
            });
        }
        sender.putMessage(BINARY, bytes, offset, length, false);
        FCSendContext.get().reset();
    }

    public void callRemoteMethod( final TopicEntry topicEntry, PacketSendBuffer sender, final int methodIndex, final Object args[], boolean loopback, boolean unreliable ) throws IOException {
        FSTObjectOutput fstObjectOutput = out.get();
        if ( fstObjectOutput == null ) {
            fstObjectOutput = new FSTObjectOutput(conf);
            out.set(fstObjectOutput);
        }
        fstObjectOutput.resetForReUse();
        if ( hasRemoteResult(args) ) {
            final FCFutureResultHandler inner = (FCFutureResultHandler) args[args.length - 1];
            final long cbid = topicEntry.getCbMap().assignCallbackId(inner);
            //FIXME: Allocation of tmp Runnables
            inner.setCbid(cbid);
            inner.setTopicEntry(topicEntry);
            args[args.length - 1] = cbid;
        }

        String rec = FCSendContext.get().getReceiver();

        fstObjectOutput.writeFByte(REMOTE_CALL);
        fstObjectOutput.writeFByte(methodIndex);

        byte flowFilter[] = FCSendContext.get().getFlowHeader();
        if ( flowFilter != null )
            fstObjectOutput.writeFByteArr(flowFilter);

        if ( rec == null )
            fstObjectOutput.writeFByte(0);
        else {
            fstObjectOutput.writeFByte(1);
            fstObjectOutput.writeStringUTF(rec);
        }
        fstObjectOutput.writeFByte(args.length);
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            fstObjectOutput.writeObject(arg);
        }

        byte[] buffer = fstObjectOutput.getBuffer();

        if ( loopback ) { //FIXME: receiver ignored ?
            topicEntry.getMethodExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    prepareReceiveContext(nodeId, topicEntry);
                    final Object lastArg = args[args.length - 1];
                    if ( args.length > 1 && lastArg instanceof FCFutureResultHandler) {
                        args[args.length-1] = new FCFutureResultHandler() {
                            volatile boolean done = false;

                            @Override
                            public void done() {
                                done = true;
                            }

                            @Override
                            public void resultReceived(Object obj, String sender) {
                                throw new RuntimeException("invoked at result receiver side. Not invokable here");
                            }

                            @Override
                            public void sendResult(Object obj) {
                                if ( ! done ) {
                                    ((FCFutureResultHandler)lastArg).resultReceived(obj,nodeId);
                                }
                            }
                        };
                    }
                    Method method = topicEntry.getMethods()[methodIndex];
                    try {
                        method.invoke(topicEntry.getService(), args);
                    } catch (IllegalAccessException e) {
                        FCLog.log(e);
                    } catch (InvocationTargetException e) {
                        FCLog.log(e);
                    }
                }
            });
        }
        sender.putMessage(-1, new HeapBytez(buffer,0,fstObjectOutput.getWritten()), 0, fstObjectOutput.getWritten(), false);
        FCSendContext.get().reset();
    }

    public FCTransportDispatcher getTransportDispatcher( String transName ) {
        FCTransportDispatcher res = dispatcher.get(transName);
        if ( res == null ) {
            Transport transport = getTransport(transName);
            res = new FCTransportDispatcher( transport, config.getClusterName(), nodeId );
            dispatcher.put(transName,res);
        }
        return res;
    }

    protected void initTopics(FCTopicConf[] channelConfs) {
        for (int i = 0; i < channelConfs.length; i++) {
            FCTopicConf channelConf = channelConfs[i];
            initTopic(channelConf);
        }

        for (int i = 0; i < channelConfs.length; i++) {
            FCTopicConf channelConf = channelConfs[i];
            if ( channelConf.isAutoStart() ) {
                startSending(channelConf.getName());
                startReceiving(channelConf.getName());
            }
        }
    }

    // just init, no send/receive
    private void initTopic(FCTopicConf channelConf) {
        this.channelConf.put(channelConf.getName(), channelConf);
        FCTransportDispatcher dispatcher = getTransportDispatcher(channelConf.getTransport());
        channelConf.getSendPauseMicros();
        TopicEntry topicEntry = new TopicEntry(channelConf);
        topicEntry.setChannelDispatcher(dispatcher);
        try {
            installService(topicEntry);
        } catch (Exception e) {
            FCLog.log(e);
        }
        topics.put(channelConf.getName(), topicEntry);
    }

    /**
     * setup proxy classes, instantiate service and setup internal hashmaps. does not joinCluster listening/sending
     * @param topic
     * @throws Exception
     */
    protected void installService(final TopicEntry topic) throws Exception {
        String serviceClazz = topic.getConf().getServiceClass();
        if ( topic.getMsgReceiver() == null )
            topic.setMsgReceiver(createMsgReceiver(topic));

        if ( serviceClazz == null ) // defined at subscribe time
            return;

        initServiceClz(topic, serviceClazz);

    }

    private void initServiceClz(TopicEntry topic, String serviceClazz) throws Exception {
        //FIXME: init completely confused since introduction of programmatic init ..
        Class clz = Class.forName(serviceClazz);
        if ( getServiceProxy(topic.getConf().getName()) == null )
            topic.setServiceProxy((FCRemoteServiceProxy) proxyFactory.createProxy(clz, topic, this));
        if ( topic.getMethods() == null )
            topic.setMethods(proxyFactory.getSortedPublicMethods(clz));
    }

    static class ReceiverThreadContext {
        long lastSenderCleanUp = System.currentTimeMillis();
        Object zeroArgs[] = new Object[0];
        Object oneArgs[] = new Object[1];
        Object twoArgs[] = new Object[2];
        Object threeArgs[] = new Object[3]; // fixme ;-)
        Object fourArgs[] = new Object[4];
    }

    ThreadLocal<ReceiverThreadContext> recCtx = new ThreadLocal<ReceiverThreadContext>() {
        @Override
        protected ReceiverThreadContext initialValue() {
            return new ReceiverThreadContext();
        }
    };

    private MsgReceiver createMsgReceiver(final TopicEntry topic) {
        // CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED CALLED MTHREADED
        return new MsgReceiver() {
            @Override
            public void messageReceived(final String sender, long sequence, Bytez bz, int off, int len) {
                ReceiverThreadContext ct = recCtx.get();
                boolean listenMethods = topic.isListenCalls();
                // major dispatch of remote methods
                try {
                    if ( listenMethods && len > 0 && bz.get(off) == BINARY ) {
                        topic.getService().receiveBinary(bz,off+1,len-1);
                        return;
                    }

                    prepareReceiveContext(sender, topic);

                    int code = bz.get(off);

                    if (listenMethods && (code == REMOTE_CALL || code == FAST_CALL) ) {
                        FCTopicService callTarget = topic.getService();
                        int methodIndex = bz.get(off+1);

                        int flowHeader = callTarget.readAndFilter(methodIndex, bz, off + 2);
                        if ( flowHeader < 0 ) {
                            return;
                        }

                        byte b[] = bz.toBytes(off,len);
                        FSTObjectInput fstObjectInput = getNewFstObjectInput(b, 0, len);

                        // skip 2 bytes+flow header. read/write asymetry
                        for (int i=0; i < flowHeader+2; i++)
                            fstObjectInput.readFByte();

                        byte isRec = fstObjectInput.readFByte();
                        if ( isRec == 1 ) {
                            String rec = fstObjectInput.readStringUTF();
                            if ( ! rec.equals(getNodeId()) )
                                return;
                        }

                        Object args[] = ct.zeroArgs;
                        int numArgs = fstObjectInput.readFByte();
                        final Method m = topic.getMethods()[methodIndex];
                        Class[] argTypes = topic.getMethodArgs()[methodIndex];

                        boolean hasRemoteResult = argTypes != null && argTypes.length > 0 && argTypes[argTypes.length - 1] == FCFutureResultHandler.class;
                        if ( numArgs > 0 ) {

                            // prepare/reuse arg array
                            if (!hasRemoteResult) {
                                switch (numArgs) {
                                    case 1: args = ct.oneArgs; break;
                                    case 2: args = ct.twoArgs; break;
                                    case 3: args = ct.threeArgs; break;
                                    case 4: args = ct.fourArgs; break;
                                    default:
                                        args = new Object[numArgs];
                                }
                            } else {
                                args = new Object[numArgs];
                            }

                            // give FCTopicService a shot to track/consume calls
                            boolean invoked = callTarget.invoke(methodIndex, m, fstObjectInput,argTypes);
                            if ( invoked ) {
                                return;
                            }

                            FCInvoker fcInvoker = topic.getMethodInvoker()[methodIndex];
                            if ( fcInvoker == null ) {
                                fcInvoker = topic.getMethodInvoker()[methodIndex] = proxyFactory.getMethod(topic.getServiceClazz(),methodIndex);
                            }
                            if ( fcInvoker != null ) {
                                fcInvoker.invoke(callTarget,fstObjectInput);
                                // invokers are only avaiable if no remote result is avaiable,
                                // safely return
                                return;
                            }

                            // decode args
                            switch (code) {
                                case REMOTE_CALL:
                                    for ( int i = 0; i < numArgs; i++ ) {
                                        args[i] = fstObjectInput.readObject();
                                    }
                                    break;
                                case FAST_CALL:
                                    decodeFastCall(fstObjectInput, argTypes, args);
                                    break;
                            }
                        }
                        if (hasRemoteResult) {
                            // replace last argument by result dispatcher
                            final Long cbId = (Long) args[args.length - 1];
                            args[args.length - 1] = createRemoteResultDispatcher(sender, cbId, topic);
                        }
                        try {
                            // slow reflection call
                            m.invoke(callTarget, args);
                        } catch (Exception e) {
                            FCLog.log(e);
                        }

                    } else if (code == CALL_RESULT) {
                        byte b[] = bz.toBytes(off+1,len-1);
                        FSTObjectInput fstObjectInput = getNewFstObjectInput(b, 0, len);
                        String receiver = fstObjectInput.readStringUTF();
//                        System.out.println("CB to "+receiver+" my nodid "+nodeId);
                        if ( nodeId.equals(receiver) ) {
                            long cbid = fstObjectInput.readFLong();
//                            System.out.println("  cbid "+cbid);
                            Object result = fstObjectInput.readObject();
                            FCFutureResultHandler fcFutureResultHandler = topic.getCbMap().get(cbid);
                            if (fcFutureResultHandler != null) {
//                                System.out.println("hit cb 1 "+cbid);
                                fcFutureResultHandler.resultReceived(result, sender);
                            } else {
//                                System.out.println("missed cb 1 "+cbid);
                            }
                        } else {
//                            System.out.println("  omitted cbid "+fstObjectInput.readFLong());
                        }
                    } else if (code == HEARTBEAT) {
//                        System.out.println("heartbeat from "+sender);
//                        if ( listenMethods ) 
                        {
                            long now = System.currentTimeMillis();
                            topic.registerHeartBeat(sender, now);
                            if ( now-ct.lastSenderCleanUp > topic.getConf().getSenderTimeoutMillis() ) {
                                List<String> timedOutSenders = topic.getTimedOutSenders(now, 30000);
                                topic.removeSenders(timedOutSenders);
                                topic.getChannelDispatcher().cleanup(timedOutSenders, topic.getTopic() );
                                ct.lastSenderCleanUp = now;
                            }
                        }
                    } else {
                        if ( listenMethods )
                            FCLog.get().severe("unknown code " + code, new Exception("stack trace"));
                    }
                } catch (Exception e) {
                    FCLog.log(e);
                }
            }
        };
    }

    // get and setup threadlocal input stream
    private FSTObjectInput getNewFstObjectInput(byte[] b, int off, int len) throws IOException {
        FSTObjectInput fstObjectInput = in.get();
        if (fstObjectInput == null) {
            fstObjectInput = new FSTObjectInput(conf);
            in.set(fstObjectInput);
            fstObjectInput.setConditionalCallback(this);
        }
        fstObjectInput.resetForReuseUseArray(b, off, len);
        return fstObjectInput;
    }

    private void prepareReceiveContext(String sender, TopicEntry topic) {
        FCReceiveContext fcReceiveContext = FCReceiveContext.get();
        fcReceiveContext.sender = sender;
        fcReceiveContext.entry = topic;
    }

    private FCFutureResultHandler createRemoteResultDispatcher(final String sender, final Long cbId, final TopicEntry topic) {
        FCFutureResultHandler res = new FCFutureResultHandler() {
            @Override
            public void sendResult(final Object obj) {
                topic.getReplys().execute(new Runnable() {
                    @Override
                    public void run() {
                        while ( !topic.hadHeartbeat(sender) ) { // wait at least for one heartbeat
                            Thread.yield();
                        }

                        FSTObjectOutput fstOut = out.get();
                        if (fstOut == null) {
                            fstOut = new FSTObjectOutput(conf);
                            out.set(fstOut);
                        }
                        fstOut.resetForReUse();
                        try {
                            fstOut.writeFByte(CALL_RESULT);
                            fstOut.writeStringUTF(sender); /* <===== receiver filtering is here*/
//                            System.out.println("result to sender "+sender+" cbid "+cbId);
                            fstOut.writeFLong(cbId);
                            fstOut.writeObject(obj);
                        } catch (IOException e) {
                            FCLog.log(e);
                        }
                        byte[] buffer = fstOut.getBuffer();
                        if ( topic.getSender() == null ) {
                            throw new RuntimeException("need to call startSending on topic '"+topic.getConf().getName()+"' in order to process method results. Topic:"+topic.getConf().getName() );
                        }
                        topic.getSender().putMessage(-1, new HeapBytez(buffer,0,fstOut.getWritten()), 0, fstOut.getWritten(), false);
                    }
                });
            }
            @Override
            public void resultReceived(Object obj, String sender) {
                throw new RuntimeException("invoked at result receiver side. Not invokable here");
            }

        };
        res.setCbid(cbId);
        res.setTopicEntry(topic);
        return res;
    }

    FCMembership memberShipRemote;
    FCMembership memberShipLocal;

    public FCMembership getMemberShipRemoteProxy() {
        return memberShipRemote;
    }

    public FCMembership getMemberShipLocal() {
        return memberShipLocal;
    }

    // optimized deserialization for flat argument lists (primitives and strings)
    private void decodeFastCall(FSTObjectInput fstObjectInput, Class[] argTypes, Object[] resultingArgs) throws IOException {
        for (int i = 0; i < argTypes.length; i++) {
            Class argType = argTypes[i];
            if (argType == boolean.class ) {
                resultingArgs[i] = fstObjectInput.readBoolean();
            } else
            if (argType == byte.class ) {
                resultingArgs[i] = fstObjectInput.readFByte();
            } else
            if (argType == short.class ) {
                resultingArgs[i] = fstObjectInput.readFShort();
            } else
            if (argType == char.class ) {
                resultingArgs[i] = fstObjectInput.readFChar();
            } else
            if (argType == int.class ) {
                resultingArgs[i] = fstObjectInput.readFInt();
            } else
            if (argType == long.class ) {
                resultingArgs[i] = fstObjectInput.readFLong();
            } else
            if (argType == float.class ) {
                resultingArgs[i] = fstObjectInput.readFFloat();
            } else
            if (argType == double.class ) {
                resultingArgs[i] = fstObjectInput.readFDouble();
            } else
            if (argType == String.class ) {
                resultingArgs[i] = fstObjectInput.readStringUTF();
            }
        }
    }

    @Override
    public boolean shouldSkip(Object o, int i, Field field) {
        return true;
    }

    protected void initTransports(FCSocketConf[] tconfs, String nodeName) {
        FCLog.log("connecting transports as '"+nodeName+"' in cluster:'"+config.getClusterName()+"'");
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
    void createTransport(FCSocketConf tconf) {
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
