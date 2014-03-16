package de.ruedigermoeller.fastcast.service;

import de.ruedigermoeller.fastcast.packeting.TopicStats;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.serialization.FSTObjectInput;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: moelrue
 * Date: 8/8/13
 * Time: 2:58 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * This service tracks other cluster nodes (which also have a FCMembership service running) and maintains a list of
 * live cluster members (including self)
 *
 */
@Unreliable
public class FCMembership extends FCTopicService {

    protected boolean doLogClusterMessages = false;

    public static interface MemberShipListener {
        public void nodeAdded(String sender, Object nodeState);
        public void nodeLost(String nodeId);
    }

    ConcurrentHashMap<String,NodePingInfo> lastPing = new ConcurrentHashMap<String, NodePingInfo>();
    List<NodePingInfo> activeNodes = new ArrayList<>();
    int heartbeatInterval = 1000;
    int timeoutAfterNIntervals = 5; // timeout after 5000
    FCMembership remote;
    MemberShipListener listener;

    volatile Object nodeState;

    public FCMembership() {
    }

    public FCMembership(int heartbeatInterval, int timeoutAfterNIntervals) {
        this.heartbeatInterval = heartbeatInterval;
        this.timeoutAfterNIntervals = timeoutAfterNIntervals;
    }

    public Object getNodeState() {
        return nodeState;
    }

    /**
     * application provided serializable object which is broadcasted.
     * Each node in the cluster maintains a list of cluster nodes and their associated nodestate
     * @param nodeState
     */
    public void setNodeState(Object nodeState) {
        this.nodeState = nodeState;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public MemberShipListener getListener() {
        return listener;
    }

    public void setListener(MemberShipListener listener) {
        this.listener = listener;
    }

    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public int getTimeoutAfterNIntervals() {
        return timeoutAfterNIntervals;
    }

    public void setTimeoutAfterNIntervals(int timeoutAfterNIntervals) {
        this.timeoutAfterNIntervals = timeoutAfterNIntervals;
    }

    @Override
    public void init() {
        remote = (FCMembership) getRemoting().getRemoteService(getTopicName());
        new Thread("Pinger") {
            @Override
            public void run() {
                while( true ) {
                    try {
                        Thread.sleep(heartbeatInterval);
                    } catch (InterruptedException e) {
                        FCLog.log(e);
                    }
                    remote.ping(System.currentTimeMillis(),nodeState);
//                    System.out.println("send ping from "+getNodeId()+" "+nodeState);
                    activeNodes = updateActiveNodes(heartbeatInterval * timeoutAfterNIntervals);
                }
            }
        }.start();
    }

    @RemoteMethod(2)
    public synchronized void getStats( String topic, FCFutureResultHandler result ) {
        TopicStats stats = getRemoting().getStats(topic);
        if ( stats != null ) {
            if ( stats.getSnapshot() == null ) {
                stats.reset();
            } else {
                if ( System.currentTimeMillis() - stats.getSnapshot().getRecordEnd() > 1000 ) {
                    stats.reset();
                }
            }
            result.sendResult(stats.getSnapshot());
            return;
        }
        result.sendResult(null);
    }

    @RemoteMethod(3)
    public synchronized void getActiveTopics(FCFutureResultHandler res) {
        List<String> activeTopics = FastCast.getRemoting().getActiveTopics();
        for (int i = 0; i < activeTopics.size(); i++) {
            String s = activeTopics.get(i);
            res.sendResult(s);
        }
    }

    @RemoteMethod(1)
    @Loopback
    public synchronized void ping(long timeSent, Object nodeStateObject) {
        String sender = FCReceiveContext.get().getSender();
        if ( lastPing.get(sender) == null ) {
            nodeAdded(sender,nodeStateObject);
        }
        lastPing.put(sender, new NodePingInfo(sender, System.currentTimeMillis(),nodeStateObject));
    }

    public static class NodePingInfo {
        long time;
        Object nodeState;
        String sender;

        public NodePingInfo(String sender, long time, Object nodeState) {
            this.time = time;
            this.nodeState = nodeState;
            this.sender = sender;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public Object getNodeState() {
            return nodeState;
        }

        public void setNodeState(Object nodeState) {
            this.nodeState = nodeState;
        }

        public String getSender() {
            return sender;
        }

        @Override
        public String toString() {
            return "NodePingInfo{" +
                    "time=" + time +
                    ", nodeState=" + nodeState +
                    ", sender='" + sender + '\'' +
                    '}';
        }
    }

    public static class MemberNodeInfo implements Serializable {
        private final int procs;
        String nodeId;
        long maxMemMB;
        String hostName;

        public MemberNodeInfo(String node) {
            nodeId = node;
            maxMemMB = Runtime.getRuntime().maxMemory()/1000/1000;
            procs = Runtime.getRuntime().availableProcessors();
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostName = "unknown";
            }
        }

        public int getProcs() {
            return procs;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public long getMaxMemMB() {
            return maxMemMB;
        }

        public void setMaxMemMB(long maxMemMB) {
            this.maxMemMB = maxMemMB;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        @Override
        public String toString() {
            return "" + nodeId+" ["+hostName+", "+maxMemMB+" MB, "+procs+" cores]";
        }
    }

    @RemoteMethod(4)
    public synchronized void getNodeInfo(FCFutureResultHandler res) {
        res.sendResult(new MemberNodeInfo(getNodeId()));
    }

    @RemoteMethod(5)
    public void clusterLog( String text ) {
        FCLog.get().internal_clusterListenerLog(FCReceiveContext.get().getSender() + ":" + text);
    }

    @Override
    protected boolean invoke(int methodIndex, Method m, FSTObjectInput in, Class[] types) {
        if ( methodIndex == 5 )
            return !doLogClusterMessages;
        return false;
    }

    protected List<NodePingInfo> updateActiveNodes(long lastPingDelay) {
        ArrayList<NodePingInfo> res = new ArrayList<>();
        for (java.util.Iterator iterator = lastPing.keySet().iterator(); iterator.hasNext(); ) {
            String next = (String) iterator.next();
            if ( System.currentTimeMillis()-lastPing.get(next).getTime() < lastPingDelay ) {
                res.add(lastPing.get(next));
            } else {
                iterator.remove();
                nodeLost(next);
            }
        }
        return res;
    }

    public List<NodePingInfo> getActiveNodes() {
        return activeNodes;
    }

    public List<NodePingInfo> getActiveNodes(String subString) {
        ArrayList<NodePingInfo> res = new ArrayList<>();
        List<NodePingInfo> copy = activeNodes;
        for (int i = 0; i < copy.size(); i++) {
            String adr = copy.get(i).getSender();
            if (subString == null || adr.indexOf(subString) >= 0) {
                res.add(copy.get(i));
            }
        }
        return res;
    }

    public NodePingInfo [] getActiveNodesOrderDeterministic(String subString) {
        List<NodePingInfo> activeNodes = getActiveNodes(subString);
        NodePingInfo hostNodeAddresses[] = new NodePingInfo[activeNodes.size()];
        Collections.sort(activeNodes, new Comparator<NodePingInfo>() {
            @Override
            public int compare(NodePingInfo o1, NodePingInfo o2) {
                return o1.getSender().compareTo(o2.getSender());
            }
        });
        for (int i = 0; i < activeNodes.size(); i++) {
            hostNodeAddresses[i] = activeNodes.get(i);
        }
        return hostNodeAddresses;
    }

    public <E> ArrayList<E> getActiveNodes(Class<E> nodeInfoClass) {
        List<NodePingInfo> activeNodes = getActiveNodes();
        ArrayList<E> res = new ArrayList<>();
        for (int i = 0; i < activeNodes.size(); i++) {
            NodePingInfo nodePingInfo = activeNodes.get(i);
            if ( nodePingInfo.getNodeState() != null && nodeInfoClass.isAssignableFrom(nodePingInfo.getNodeState().getClass()) ) {
                res.add((E) nodePingInfo.getNodeState());
            }
        }
        return res;
    }

    public String [] getActiveNodeAdressesOrderDeterministic(String subString) {
        List<NodePingInfo> activeNodes = getActiveNodes(subString);
        String hostNodeAddresses[] = new String[activeNodes.size()];
        Collections.sort(activeNodes, new Comparator<NodePingInfo>() {
            @Override
            public int compare(NodePingInfo o1, NodePingInfo o2) {
                return o1.getSender().compareTo(o2.getSender());
            }
        });
        for (int i = 0; i < activeNodes.size(); i++) {
            hostNodeAddresses[i] = activeNodes.get(i).getSender();
        }
        return hostNodeAddresses;
    }

    public String dumpToString() {
        List nodes = activeNodes;
        StringBuffer res = new StringBuffer();
        res.append("----------------------------------------\n");
        for (int i = 0; i < nodes.size(); i++) {
            Object o = nodes.get(i);
            res.append("Node: " + o + "\n");
        }
        res.append("----------------------------------------\n");
        return res.toString();
    }

    public void dump() {
        FCLog.log(dumpToString());
    }

    public void nodeAdded(String sender, Object nodeState) {
        if ( listener != null )
            listener.nodeAdded(sender,nodeState);
    }

    public void nodeLost(String lostNode) {
        if ( listener != null )
            listener.nodeLost(lostNode);
    }

    public boolean isDoLogClusterMessages() {
        return doLogClusterMessages;
    }

    /**
     * if true, this node receives cluster messages and logs them to the local logger with level 'FCLog.CLUSTER_LISTENER'
     * @param doLogClusterMessages
     */
    public void setDoLogClusterMessages(boolean doLogClusterMessages) {
        this.doLogClusterMessages = doLogClusterMessages;
    }
}
