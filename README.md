fast-cast
=========


High performance low latency topic/stream based reliable UDP messaging ("event-bus").

**3.x** is in the making. Old remote method layer has been abandonned (will be covered by konktraktor remote actors later on).

**3.0 features**:
- Throughput **7 million 70 bytes msg/second** on localhost (Intel i7 or newer XEONS). Similar rates can be achieved using 10GBit high end networks (see examples, structencoding).
- **reliable low latency with extraordinary few outliers**. E.g. XEON 3Ghz, CentOS 6.5 RT Linux: RTT latency mean:12 micros, 99.9% - 24 micros, 99.99% - 111 micros, 99.9999% - 126 micros. Testscenario was Ping-Pong RTT latency.
- transparent fragmentation and defragmentation of **large messages** (should not exceed 50-70% of publisher  history buffer and subscribers's receive buffer).
- **add hoc unicast** (publisher can address all subscribers or a single subscriber on a per message level). Eases request/response schemes. 
- **bidirectional** a process can both send and receive on a topic if required.
- support for up to 256 **topics** per address/port.
- supports **fully reliable** as well as unreliable streams (unordered-reliable streams coming soon)
- API exposes raw **low-level zero copy** interface as well as **higher level** fast-serialized sendObject/receiveObject utilities
- supports both **blocking IO** (saves CPU) and **lock free poll** mode (low latency, CPU/cores burned)
- all buffers are kept **off heap** to avoid GC pressure.
- **allocation free** in the main path
- designed for **use in LAN clusters**.
- simple implementation + algorithm, flow control is based on static rate limiting for now.

check out examples folder and tests on how to use fc. Documentation pending .. this is beta software

initial release is available on maven.
```
<dependency>
    <groupId>de.ruedigermoeller</groupId>
    <artifactId>fast-cast</artifactId>
    <version>3.02</version>
</dependency>
```

Changes done from 2.x to 3.x:
- removed remote method framework completely (will be replaced by kontraktor actors on top of fast-cast). This will  reduce exposure to bugs and also reduces impl complexity.
- refurbished core NAK UDP streaming implementation. Renamed many classes to improve understandability
- simplified API significantly
- 3.0 has been optimized for low latency (2.x is a bastard latency wise ..). 
- requires fast-serialization 2.17 branch build for struct support
- allocation free under normal operation

