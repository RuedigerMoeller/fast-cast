fast-cast
=========


High performance low latency topic/stream based reliable UDP messaging ("event-bus").

**3.x** is in the making. Old remote method layer has been abandonned (will be covered by konktraktor remote actors later on).

**3.0 features**:
- all buffers are kept **off heap** to avoid GC pressure.
- **allocation free** in the main path
- supports both **blocking IO** (save CPU) and **lock free poll** mode (low latency, CPU/cores burned)
- support for up to 256 topics per address/port.
- transparent fragmentation and defragmentation of **large messages** (should not exceed 50-70% of publisher send history buffer and subscribers's receivebuffer. E.g. an 80 MB message is not an issue with appropriate sizing.
- **add hoc unicast** (publisher can address all subscribers or a single subscriber on a per message level). Eases request/response schemes. 
- supports **fully reliable** as well as unreliable streams (unordered-reliable streams coming soon)
- API exposes raw **low-level zero copy** interface as well as **higher level** ease of use fast-serialized sendObject/receiveObject utilities
- Low latency: very few outliers. Depending on hardware/OS RTT latencies of 12 micro seconds (mean),99.9% - 24 micros, 99.99% - 111 micros, 99.9999% - 126 micros. Regarding outliers fast-cast currently beats any message layer I have tested (both commercial and open source) by a good margin (in a clean network with rare packet loss + low latency hardware and OS setup). Testscenario is Ping-Pong RTT latency.
- simple implementation + algorithm, flow control is based on static rate limiting for now.
- detailed throughput tests still open, expect >3 million 50 bytes msg/second on decent hardware with appropriate configuration.
- designated for **use in LAN clusters**.

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

