package org.nustaq.fastcast.api;

import org.nustaq.fastcast.config.*;
import org.nustaq.fastcast.impl.TransportDriver;
import org.nustaq.fastcast.util.FCLog;
import org.nustaq.fastcast.util.FCUtils;
import org.nustaq.fastcast.impl.*;
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

    static {
        FSTStructFactory.getInstance().registerSystemClz((byte)127, Packet.class, DataPacket.class, RetransPacket.class, RetransEntry.class, ControlPacket.class);
    }

    static FastCast fc;
    private String clusterName = "-";
    private ClusterConf config;

    public static FastCast getFastCast() {
        synchronized (FastCast.class) {
            if ( fc != null ) {
                return fc;
            }
            fc = new FastCast();
            FCLog.get().internal_clusterListenerLog(
                    "____ ____ ____ ___ ____ ____ ____ ___\n" +
                    "|--- |--| ====  |  |___ |--| ====  |  \n" + "> v3"
            );
            fc.setNodeId("N"+(int)(Math.random()*1000));
            return fc;
        }
    }

    protected HashMap<String,PhysicalTransport> transports = new HashMap<String, PhysicalTransport>();
    protected HashMap<String,TransportDriver> drivers = new HashMap<String, TransportDriver>();
    String nodeId;

    public void setNodeId(String nodeName) {
        if ( nodeId != null )
            throw new RuntimeException("Node Id can only be set once per process");
        nodeId = FCUtils.createNodeId(nodeName);
    }

    public String getNodeId() {
        return nodeId;
    }

    public PhysicalTransport getTransport(String name) {
        PhysicalTransport physicalTransport = transports.get(name);
        if ( physicalTransport == null ) {
            FCLog.log("could not find transport '" + name + "'. Falling back to transport 'default'");
            return transports.get("default");
        }
        return physicalTransport;
    }

    public TransportDriver getTransportDriver(String transName) {
        TransportDriver res = drivers.get(transName);
        if ( res == null ) {
            PhysicalTransport physicalTransport = getTransport(transName);
            res = new TransportDriver(physicalTransport, clusterName, nodeId );
            drivers.put(transName, res);
        }
        return res;
    }

    public void loadConfig(String filePath) throws Exception {
        setConfig(ClusterConf.readFrom(filePath));
    }

    public void setConfig(ClusterConf config) {
        this.config = config;
        addTransportsFrom(config);
    }

    /**
     * only avaiable if initialized with setConfig or loadConfig
     * @param name
     * @return
     */
    public SubscriberConf getSubscriberConf(String name) {
        TopicConf topic = getConfig().getTopic(name);
        if ( topic != null )
            return topic.getSub();
        return null;
    }

    /**
     * only avaiable if initialized with setConfig or loadConfig
     * @param name
     * @return
     */
    public PublisherConf getPublisherConf(String name) {
        TopicConf topic = getConfig().getTopic(name);
        if ( topic != null )
            return topic.getPub();
        return null;
    }

    public ClusterConf getConfig() {
        return config;
    }

    public static class ConfigurationAlreadyDefinedException extends RuntimeException {
        public ConfigurationAlreadyDefinedException(String message) {
            super(message);
        }
    }

    public void addTransportsFrom(ClusterConf config) {
        PhysicalTransportConf[] trs = config.transports;
        for (int i = 0; i < trs.length; i++) {
            PhysicalTransportConf tr = trs[i];
            addTransport(tr);
        }
    }

    /**
     *
     * @param tconf
     */
    public void addTransport(PhysicalTransportConf tconf) {
        if ( nodeId == null )
            throw new RuntimeException("define nodeId first");
        if ( transports.get(tconf.getName()) != null ) {
            throw new ConfigurationAlreadyDefinedException("transport "+tconf.getName()+" already initialized ");
        }
        try {
            FCLog.log("Connecting transport " + tconf.getName());
            MulticastChannelPhysicalTransport tr = new MulticastChannelPhysicalTransport(tconf);
            tr.join();
            transports.put(tconf.getName(), tr);
        } catch (IOException e) {
            FCLog.log(e);
        }
    }

}
