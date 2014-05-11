package de.ruedigermoeller.fastcast.config;

import de.ruedigermoeller.fastcast.service.FCMembership;
import de.ruedigermoeller.fastcast.transport.FCSocketConf;
import de.ruedigermoeller.fastcast.util.FCLog;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/13/13
 * Time: 12:38 AM
 *
 * A helper class to build up cluster configurations programmatically. This is useful for uncritical small applications
 * e.g. simple one publisher-many receiver or 1:1 local interprocess communication.
 *
 * Note that it is possible to generate a config file from a configbuilder (however this will be formatted ugly and
 * also redefine all default values).
 *
 * Warning 1: for larger, performance critical clusters it is strongly recommended to use config files.
 * Warning 2: all cluster nodes should share identical configurations, else you might experience communication chaos. Better
 *            create some static method initializing a cluster conf, which is used by all nodes of your cluster.
 *
 */
public class FCConfigBuilder {

    public static FCConfigBuilder New() {
        return new FCConfigBuilder();
    }

    String nodeName;
    ArrayList<FCSocketConf> trans = new ArrayList<>();
    ArrayList<FCTopicConf> topics = new ArrayList<>();
    int loglevel = FCLog.INFO;

    public FCConfigBuilder loglevel(int levelFCLog) {
        loglevel = levelFCLog;
        return this;
    }

    public class TopicBuilder {
        String transName;

        public TopicBuilder(String transportName) {
            transName = transportName;
        }

        /**
         * add a topic with default values (8Mb/second rate limit, 40Mb send history).
         * You should consider using more parameterized methods below, cause defaults
         * eat up unnecessary memory for low traffic topics and in case of low-pause garbage collectors,
         * vice versa set a hard rate limit of 8MB/second which might be too low for high volume
         * topics.
         *
         * @param topicname - name used to retrieve topic related stuff later on
         * @param topicId - 0 .. FCDispatcher.MAX_NUM_TOPICS (256 currently)
         * @return
         */
        public TopicBuilder topic( String topicname, int topicId ) {
            return topic( topicname, topicId, 1000, 5);
        }

        /**
         * add a topic
         *
         * @param topicname - name used to retrieve topic related stuff later on
         * @param topicId - 0 .. FCDispatcher.MAX_NUM_TOPICS (256 currently)
         * @param maxDatagramsPerSecond - a DataGram has a default size of 8k, so a value of 1000 sets 8MByte rate limit
         *                              note that history size is by default 5*send rate
         * @return
         */
        public TopicBuilder topic( String topicname, int topicId, int maxDatagramsPerSecond ) {
            return topic(topicname, topicId, maxDatagramsPerSecond,5);
        }

        /**
         *
         * add a topic
         *
         * @param topicname - name used to retrieve topic related stuff later on
         * @param topicId - 0 .. FCDispatcher.MAX_NUM_TOPICS (256 currently)
         * @param maxDatagramsPerSecond - a DataGram has a default size of 8k, so a value of 1000 sets 8MByte rate limit
         *                              note that off heap history size is set to maxGCPauseSeconds * send rate. This consume
         *                              a lot of memory in case you set up many topics with high throughput
         * @param maxGCPauseSeconds - the maximum GC pause you expect from any receiver. Because the receiver misses packets in case of GC,
         *                          each sender has to keep a send history spanning the maximum GC pause time, else the receivers might
         *                          get dropped (unrecoverable packet loss).
         * @return
         */
        public TopicBuilder topic( String topicname, int topicId, int maxDatagramsPerSecond, int maxGCPauseSeconds ) {
            FCTopicConf conf = new FCTopicConf();
            conf.setTransport(transName);
            conf.setName(topicname);
            conf.setTopic(topicId);
            conf.setDGramRate(maxDatagramsPerSecond);

            conf.setMaxSendPacketQueueSize( maxDatagramsPerSecond/10 );

            // do 250 ms history on heap
            conf.setNumPacketHistory( maxDatagramsPerSecond*maxGCPauseSeconds+conf.getMaxSendPacketQueueSize() );
            conf.setReceiveBufferPackets(maxDatagramsPerSecond);
            topics.add(conf);
            return this;
        }

        public TopicBuilder setOpenCalls( int maxOpen, long timeoutMS) {
            FCTopicConf fcTopicConf = topics.get(topics.size() - 1);
            fcTopicConf.setMaxOpenRespondedCalls(maxOpen);
            fcTopicConf.setResponseMethodsTimeout((int) timeoutMS);
            return this;
        }

        public TopicBuilder setSendQueuePercentage( int percentageOfSendHistory) {
            if ( percentageOfSendHistory > 50 || percentageOfSendHistory < 5) {
                throw new RuntimeException("illegal arg, use 5..50");
            }
            FCTopicConf fcTopicConf = topics.get(topics.size() - 1);
            int maxDatagramsPerSecond = fcTopicConf.getNumPacketHistory();
            fcTopicConf.setMaxSendPacketQueueSize( maxDatagramsPerSecond/percentageOfSendHistory );
            return this;
        }

        /**
         * set's request respons options on last added topic. Note the more unresponded ('open') requests are allowed, the more memory is needed
         * to map requestId's to your responding future.
         *
         * On the other hand, throughput depends on the maximum allowed open requests. Default is 10000
         *
         * @param maxOpenRequests - the number of concurrent unresponded requests. If this is exceeded, calling remote methods with
         *                        result will block until new slots get avaiable either by timeout or incoming responses.
         * @param timeOutMillis - time until waiting for a response is given up. Since FastCast provides 1:N request/responses one
         *                      can receive several responses (in different thread is configured so).
         *                      In case a receiver is not interested in further responses, it should call 'done' on the
         *                      FCFutureResultHandler future object.
         */
        public TopicBuilder setRequestRespOptions( int maxOpenRequests, int timeOutMillis ) {
            FCTopicConf fcTopicConf = topics.get(topics.size() - 1);
            fcTopicConf.setMaxOpenRespondedCalls(maxOpenRequests);
            fcTopicConf.setResponseMethodsTimeout(timeOutMillis);
            return this;
        }
        /**
         * add predefined FCMembership topic service.
         * This can be used to find out about other nodes in the cluster, get notified of new/leaving nodes.
         * Additionally this is required to use the clusterview gui.
         *
         * @param topicname
         * @param topicId
         * @return
         */
        public TopicBuilder membership( String topicname, int topicId ) {
            FCTopicConf conf = new FCTopicConf();
            conf.setTransport(transName);
            conf.setName(topicname);
            conf.setTopic(topicId);
            conf.setServiceClass(FCMembership.class.getName());
            conf.setDGramRate(10);
            conf.setNumPacketHistory( 100 );
            conf.setReceiveBufferPackets( 20 );
            conf.setMaxSendPacketQueueSize( 5 );
            conf.setAutoStart(true);
            topics.add(conf);
            return this;
        }

        public FCConfigBuilder end() {
            return FCConfigBuilder.this;
        }

        /**
         * set datagram size on current transport. Default is 8000, min = 4000 max = 32000
         * @param sizBytes
         */
        public TopicBuilder datagramSize(int sizBytes) {
            trans.get(trans.size()-1).setDgramsize(sizBytes);
            return this;
        }


    }

    /**
     * initialize a transport with given name with given network adapter, multicast ip and multicast port.
     *
     * note only ip 4 is supported (use -Djava.net.preferIPv4Stack=true in case).
     * The ip's identifying a network adapter can be found on windows with 'ipconfig' under linux with 'ifconfig'.
     * Note that under Linux one can use symbolic names such as 'eth0' or 'bond0'. This does not work always with windows.
     *
     * @param transportName
     * @param networkIface - network adapter ip as told by ipconfig (win) or ifconfig (linux). Under linux on can also use the
     *                     symbolic name given (e.g. 'lo', 'eth0', 'bond0'. Not that FCClusterConfig provides a method to introduce
     *                     another level of indirection by adding 'name' to interface ip symbolic interface names
     * @param multicastIp
     * @param multicastPort
     */
    public TopicBuilder socketTransport(String transportName, String networkIface, String multicastIp, int multicastPort) {
        FCSocketConf sc = new FCSocketConf(transportName);
        sc.setIfacAdr(networkIface);
        sc.setMcastAdr(multicastIp);
        sc.setPort(multicastPort);
        trans.add(sc);
        return new TopicBuilder(transportName);
    }

    /**
     * initialize a shared memory transport with shared memory residing in mmapped file given
     *
     * @param transportName
     * @param file
     */
    public TopicBuilder sharedMemTransport(String transportName, File file) {
        return sharedMemTransport(transportName, file, 200, 16000);
    }

    /**
     * initialize a shared memory transport with shared memory residing in mmapped file given.
     * Size of file should be at least 50MB (default is 200MB). packetSize should vary from 4000 (lower throupghput, but
     * possibly lower latency) to 32000 (higher throughput). Best throughput is machine/OS dependent. 16000 to 24000 is a good
     * shot most of the time.
     *
     * @param transportName
     * @param file
     */
    public TopicBuilder sharedMemTransport(String transportName, File file, int sizeMByte, int packetSizeBytes) {
        FCSocketConf conf = new FCSocketConf(transportName);
        file.getParentFile().mkdirs();
        conf.setQueueFile(file.getAbsolutePath());
        if ( packetSizeBytes < 3000 || packetSizeBytes > 32000 ) {
            packetSizeBytes = 8000;
        }
        conf.setDgramsize(packetSizeBytes);
        conf.setSendBufferSize(sizeMByte*1000*1000);
        conf.setTransportType(FCSocketConf.MCAST_IPC);
        trans.add(conf);
        return new TopicBuilder(transportName);
    }

    /**
     * compiles a FastCast cluster configuration object from the current state
     * @return
     */
    public FCClusterConfig build() {
        FCClusterConfig conf = new FCClusterConfig();
        HashSet doubles = new HashSet();
        FCSocketConf sockets[] = new FCSocketConf[trans.size()];
        for (int i = 0; i < sockets.length; i++) {
            String name = trans.get(i).getName();
            if ( doubles.contains(name) )
                throw new RuntimeException("transport "+name+" configured twice");
            sockets[i] = trans.get(i);
            doubles.add(name);
        }
        conf.setTransports(sockets);
        FCTopicConf tops[] = new FCTopicConf[topics.size()];
        doubles.clear();
        for (int i = 0; i < tops.length; i++) {
            String name = topics.get(i).getName();
            int topicId = topics.get(i).getTopic();
            if ( doubles.contains(name) || doubles.contains(topicId) ) {
                throw new RuntimeException("Topic name or Id defined twice "+topicId);
            }
            tops[i] = topics.get(i);
            doubles.add(name);
            doubles.add(topicId);
        }
        conf.setTopics(tops);
        conf.setLogLevel(loglevel);
        return conf;
    }

}
