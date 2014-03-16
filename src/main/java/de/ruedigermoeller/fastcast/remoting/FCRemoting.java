package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.config.FCClusterConfig;
import de.ruedigermoeller.fastcast.config.FCTopicConf;
import de.ruedigermoeller.fastcast.packeting.TopicStats;
import de.ruedigermoeller.fastcast.service.FCMembership;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 23.08.13
 * Time: 01:42
 * To change this template use File | Settings | File Templates.
 */
public interface FCRemoting {

    /**
     * @return cluster-unique nodeid associated with this process. Avaiable after joinCluster !
     */
    String getNodeId();

    /**
     * return the local instance of a service with given name.
     * via getService(name).getProxy() the Remote Proxy can be obtained to send messages to remote instances.
     * The service is present only if startReceiving has been called on the associated topic
     * @param topic
     * @return
     */
    FCTopicService getService(String topic);

    /**
     * return the remote service proxy object. Calling @remote annotated methods on this will send to all receivers
     * listening to the service in the cluster.
     *
     * @param topic
     * @return
     */
    FCRemoteServiceProxy getRemoteService(String topic);

    /**
     * joins the cluster defined by the given config. Note that listening/sending must be explicitely enabled
     * after joining.
     *
     * clustername is a Development convenience feature. One can start several clusters with different name without
     * having to reconfigure ports/adresses etc. . However since those clusters share the same network queues/ports,
     * this will slow down communication. DON'T USE FOR PRODUCTION SYSTEMS. In a production system one would run
     * more than one clsuter at a time by using different ports and multicast adresses, so packet filtering is done
     * by the network hardware/kernel.
     *
     * @param configPath
     * @param nodeId - name of the process (will make up the adress of a node e.g. as reported in senderId). MUST NOT EXCEED 8 chars.
     * @param clusterName - see above. If null, take clustername from config. MUST NOT EXCEED 8 chars.
     * @throws IOException
     */
    void joinCluster(String configPath, String nodeId, String clusterName) throws IOException;
    void joinCluster(FCClusterConfig config, String nodeId, String clusterName) throws IOException;

    void start(String serviceName);

    /**
     * starts sending on the given topic
     * @param topicName
     */
    FCRemoteServiceProxy startSending(String topicName);
    FCRemoteServiceProxy startSending(String topic, Class<? extends FCTopicService> fcBinaryTopicServiceClass) throws Exception;
    void stopReceiving(String topicName);

    /**
     * starts listening on the given topic
     * @param topicName
     */
    void startReceiving(String topicName);
    void startReceiving(String topicName, FCBinaryMessageListener listener);
    void startReceiving(String topicName, FCTopicService listener);

    TopicStats getStats(String name);


    FCTopicConf getTopicConfiguration(String name);

    List<String> getActiveTopics();

    FCRemotingListener getRemotingListener();
    public void setRemotingListener(FCRemotingListener listener);

    /**
     * returns remote FCMemberShip instance, null if none installed
     * @return
     */
    FCMembership getMemberShipRemoteProxy();

    /**
     * @return the local membership instance if any
     */
    FCMembership getMemberShipLocal();

}
