fast-cast
=========


High performance low latency topic/stream based reliable UDP messaging ("event-bus").

Note: 2.x Old remote method layer has been abandonned (will be covered by future kontraktor releases)

**3.0 features**:
- Throughput up to **7 million 70 bytes msg/second** (Intel i7 or newer XEONS, 10Gb network or localhost).
- **reliable low latency with extraordinary few outliers**. Testscenario: Ping-Pong RTT latency. XEON 3Ghz, CentOS 6.5 RT Linux: RTT latency mean:12 micros, 99.9% - 24 micros, 99.99% - 111 micros, 99.9999% - 126 micros. 
- transparent fragmentation and defragmentation of **large messages** (max 50% of publisher history buffer and < subscribers's receive buffer).
- **ad hoc unicast** (publisher can address all subscribers or a single subscriber on a per message level).
- supports **fully reliable** as well as unreliable streams (unordered-reliable streams coming soon)
- **blocking IO** (saves CPU) and **lock free poll** mode (low latency, CPU/cores burned)
- all buffers are kept **off heap** to avoid GC pressure.
- **allocation free** in the main path
- requires **JDK 1.7** or higher

check out examples folder and tests on how to use fc. This is beta software

initial release is available on maven.
```
<dependency>
    <groupId>de.ruedigermoeller</groupId>
    <artifactId>fast-cast</artifactId>
    <version>3.08</version>
</dependency>
```

Changes done from 2.x to 3.x:
- removed remote method framework completely (will be replaced by kontraktor actors on top of fast-cast). This will  reduce exposure to bugs and also reduces impl complexity.
- refurbished+refactored core NAK UDP streaming implementation.
- simplified API
- 3.0 has been optimized for low latency (2.x is a bastard latency wise ..). 
- requires fast-serialization 2.17 branch build for struct support
- allocation free under normal operation

Documentation
===================

**Multicast**

Fastcast uses ip4 multicast. This means a publisher sends a packet once, which is then received by all subscribers. This can be advantageous e.g. for high avaiability or broadcasting of common state changes across a cluster of processes. Multicast networking scales better compared to connection based tcp clusters, as messages don't have to be sent multiple times on distinct connections (e.g. for HA, broadcast). Additionally there is no latency caused by TCP connection creation and TCP backtalking receiver=>sender (e.g. ACK, flow control). 

Multicast addresses start at 224.0.0.0, however its recommended to use addresses > 225.0.0.0. Do not rely on address, its also important which port is chosen (avoid "crosstalking"). Ideally choose a distinct addr and distinct port for each 'transport' (see below 'terminology') used. 
With increasingily defensive configuration defaults, getting multicast to run on a network can be pretty time consuming. The following things are usually problematic:
* rp_filter of linux kernel (reverse filtering fails because multicast packet can have weird sender address). E.g. RH7
* firewall defaults
* disabled at network adapter level
* traffic shaping switches defaults: limited bandwidth for multicast traffic
* complex network setups with slow network segments attached might backpressure multicast traffic accross the whole network. E.g. an attached 100MBit or wireless lan segment might cause multicast traffic in the 1GBit lan to slow down to wireless network speed.
* IGMP behaviour, buggy IGMP implementations (first message not correctly routed, ..).

ethtool, tcpdump, and netstat are your diagnostic helpers ..

**Reliability Algorithm used by fast-cast**

Fastcast employs pure NAK. A *publisher* keeps a sequence and history for packets sent. A *subscriber* keeps a last-received-sequence and a *receive buffer* per publisher (so multiple publishers on same topic/addr:port are supported).
Once the *subscriber* detects a gap it waits a short time if the gap fills (e.g. just reordered packet). If it does not get filled it sends a retransmission broadcast (targeted to the sender id). The *publisher* then resends the missing packet(s). Once the *subscriber* can close the gap, processing can be continued with buffered packets. To clearify: Packets received while retransmission request is in flight, are buffered in the *receive buffer*, so in case the missing packet arrives, buffered packets usually allow for further processing without new gaps.
So two buffer sizes are important:
- history buffer (num_datagrams) of *publisher*
- receive buffer (num_datagrams) of *subscriber*
The higher the throughput and the longer you expect processes to stall (e.g. GC) the larger the publisher history buffer must be sized.
The higher the throughput and the higher the latency of your network, the higher the receive buffer must be sized (receive buffer should be able to buffer number of packets sent while a retransmission request/response is in flight). As retransmission requests implicitely lower the send rate of a publisher, a too low setting of receive buffers might hamper throughput in case packet loss occurs, but its not that critical for overall stability.
Once a publisher overruns a subscriber such that the subscriber wants a retransmission on an old packet which is already out of the senders history ring buffer, the subscriber gets a signal (see FCSubscriber interface) informing it cannot recover the requested messages. Message loss has happened. A Subscriber might rejoin (=reinit) or exit then.

**Flow control**

Fast cast is configured by plain limit rating (number of "packets" [datagrams] per second). However retransmission responses sent by a publisher implicitely lower its send rate.

**Batching**

The message send call ("offer") has flag determining wether the data should be sent immediately (flush) or if batching should be applied. If  'no flush' is choosen and no further message is offered, an automatic flush will be triggered after (configurable) one millisecond. If 'flush' is set to true and the publisher is near its configured packet rate limit, batching will be applied regardless of 'flush' flag. This way one can achieve that low rate traffic is sent with low latency, however once traffic bursts occur, batching will avoid backpressure onto publishing thread as long bursts can be compensated by batching.
Its recommended to always set this flag to false except there are microsecond level latency requirements.

**Packet size**

With 'packet' actually a fast-cast level 'datagram' is meant. For lowest latency choose a packet size slightly lower than netork MTU. For high throughput choose larger packet sizes (up to 65k). Downside of large packet sizes is, that a packet gap has worse effects (because e.g. 64k need to be retransmitted instead of just 1k). As history and receive buffers reserve N*full packet size number of bytes, large packets also increase required memory to hold buffers. Its good practice to choose multiples of MTU for packet sizes, though its not that significant. Usual values are 1.5k, 3k, 8k, 16k . 64k are also a possible setting (but large buffers). Recommendation is 4k to 8k. For low latency requirements set small mtu sizes on your network adapter and a packet size fitting into a single mtu size.

**large messages**

Large messages are automatically fragmented/defragmented. A message cannot be larger than a subscribers receive buffer, and not larger than a publishers send history (give at least 10%-20% headrooom).
Expect serious throughput hiccups with very large messages (>40MB and higher), especially if processes have been started and are not yet warmed up (JIT optimization hasn't kicked in yet). Once hotspot has warmed up code, even large (>80MB) messages should pass smoothly.

**configuration recommendation**

start with low packet per second rate (5000 to 10000) and moderate packet size (e.g. 4..8k). History buffer should cover at least 3-5 seconds (java JIT hiccups on newly joining processes, GC). E.g. packet send rate = 5000, 8k buffers => history for 5 seconds = 5*5000 = 25000 = (multiplied with packet size) 200MB. Receivebuffer ~1-2 seconds of traffic = 10_000 packets. For lowest latency try to push packets per second to the limit of your setup (network, CPU, OS). The more packets can be sent, the better the latency even for high message rates. Typically not more than 10000 packets @1400 packet size are processed. On high end hardware+OS I could run with up to 50k PPS setting (1400 packet size); tuned linux stack up to 100k pps, open onload kernel bypass stacks up to 150k. Note that benchmark/nic manufacturs max pps are not reachable, as you'd like to target a pps where packet losses occur rarely (< each 20 seconds).
Ensure your PPS * packet size does not exceed your network bandwidth. If latency is of little interest, choose large packets 4 to 16k) using a low rate limit (like 5000..10000):

 *Ensure subscribers do not block the receiving thread !!!!!!!*  (**!!!!**)

**API**

see org.nustaq.fastcast.api (.util) package + examples project directory. Note that I occasionally use those examples for private test setups, so you might need to tweak config files/setup to your hardware/nic. Especially if your not operating on 16 core machines using kernel bypassed low lat nic's =).

**Terminology**

A 'Transport' is a multicast address+port. A transport can be divided into up to 256 topics. A publishers sends on TopicId:transport. Note that topic traffic is still received and filtered by fast cast. So for high throuput or low latency topics its recommended to use a dedicated transport (filtering done by network hardware then). Note this can be easily changed at config level, so for dev you might want to use one transport (mcast-addr:port). In production you prefer a dedicated transport per topic.

**NodeId, Unicast**

Each node is assigned a unique id. If null is provided as a receiver in the offer method, all subscribers will receive the message sent. If a nodeid is provided, only the specific node will receive the packet. Note that if one alternates quickly in between receiverIds or 'null', batching might suffer, as the receiver id is set+filtered on packet level, not message level.

**Multithreading**

Fast-cast has one receiver thread per 'transport'. The receive callback called on the subscriber is called directly in this thread, so its very important to either process extremely quick or delegate processing to another thread (see org...fastcast.api.util for examples).
Note that the byte's of a message given in subscriber callback are valid only until the callback finishes. If message processing should be done in a secondary thread, the bytes need to get copied. This way its possible to filter out messages without the need to allocate and copy byte arrays.
Additionally there is a housekeeping thread responsible for heartbeating and 'auto-flushing'.

If the offer on the FCPublisher is called to send messages, the sending happens directly using the calling thread. Though offer is threadsafe, its not recommended to send at high rate from different threads (no protection against contention).

The most common error is to block the receiver thread by decoding and processing the message *and* send to other topics inside the receiver thread. Once send is blocked, the receiver threads get blocked and packet loss + retransmission occurs. So take care when doing cascaded network calls (B receives from A => sends to C in message receiver thread)

**Low Latency**

for ultimate low latency:
* use allocation free encoding/decoding (e.g. structs as shown in https://github.com/RuedigerMoeller/fast-cast/tree/3.0/examples/src/main/java/org/nustaq/fastcast/examples/multiplestructs).
Additionally be fast enough to get proccessing done inside receiver thread or use a fast+allocation free multithreading framework like disruptor for message processing. JDK's executor framework is not that well suited.
* choose a small datagram size (size of MTU)
* try to push PPS at least to 20k (depends on OS, network hardware). 100-150k are possible with rare retransmissions if kernel + network stack is tuned accordingly (10GBit network).
* use busy spin for receiver thread (configurable)
* use busy spin for sender thread (that's your app)
* pin threads to cores/cpu sockets
* always set 'flush' flag to true when calling offer
* use small packet sizes on the nic used for low lat mutlicast. Large MTU's are bad latency wise, they only help in throughput. (e.g. localhost pseudo nic often defaults to 16-64k). Smallest possible datagram size of fast cast is  ~65ß..700 currently [determined by max size of retransmission request structure]

Note that the biggest challenge is to get your OS configured for low latency. E.g. stock CentOS 7 has like 400 micros RTT even on local host (64k mtu!), stock ubuntu 14.04 like 20 microseconds. A well configured machine can have like 10-11 micros RTT latency on localhost, high end network hardware can have even better RTT times in case.


