package de.ruedigermoeller.fastcast.control;

import de.ruedigermoeller.fastcast.packeting.MsgReceiver;
import de.ruedigermoeller.fastcast.packeting.PacketReceiveBuffer;
import de.ruedigermoeller.fastcast.packeting.TopicEntry;
import de.ruedigermoeller.fastcast.remoting.FCRemotingListener;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.util.FCUtils;
import de.ruedigermoeller.heapoff.structs.structtypes.StructString;

import java.util.HashMap;
import java.util.concurrent.Executor;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 14.08.13
 * Time: 00:42
 * To change this template use File | Settings | File Templates.
 */
public class ReceiveBufferDispatcher {

    Executor topicWideDeliveryThread;
    HashMap<StructString,PacketReceiveBuffer> bufferMap = new HashMap<StructString, PacketReceiveBuffer>();

    int packetSize;
    String clusterName;
    String nodeId;
    int historySize;
    int topic;
    MsgReceiver receiver;
    TopicEntry topicEntry;

    public ReceiveBufferDispatcher(int packetSize, String clusterName, String nodeId, TopicEntry entry, MsgReceiver rec) {
        receiver = rec;
        this.packetSize = packetSize;
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.historySize = entry.getConf().getReceiveBufferPackets();
        this.topic = entry.getTopic();
        topicEntry = entry;
        topicWideDeliveryThread = FCUtils.createIncomingMessageThreadExecutor("global delivery " + topicEntry.getName(), topicEntry.getConf().getDecodeQSize() );
    }

    public TopicEntry getTopicEntry() {
        return topicEntry;
    }

    public PacketReceiveBuffer getBuffer(StructString sender) {
        PacketReceiveBuffer receiveBuffer = bufferMap.get(sender);
        if ( receiveBuffer == null ) {
            receiveBuffer = new PacketReceiveBuffer(packetSize,clusterName,nodeId,historySize,sender.toString(), topicEntry, receiver, topicWideDeliveryThread);
            bufferMap.put((StructString) sender.createCopy(),receiveBuffer);
        }
        return receiveBuffer;
    }

    /**
     * if a sender stops sending => remove from map to free memory
     * @param s
     */
    public void cleanup(String s) {
        StructString struct = new StructString(s);
        PacketReceiveBuffer packetReceiveBuffer = bufferMap.get(struct);
        bufferMap.remove(struct);
        packetReceiveBuffer.terminate();
        FCRemotingListener remotingListener = FastCast.getRemoting().getRemotingListener();
        if ( remotingListener != null )
            remotingListener.senderDied(topicEntry.getTopic(), topicEntry.getName(), s);
    }
}
