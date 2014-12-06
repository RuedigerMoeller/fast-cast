fast-cast
=========


High performance low latency topic/stream based reliable UDP messaging ("event-bus").

3.x is in the making, see Wiki for documentation for old 2.x release. 2.x is available at maven.

Changes done from 2.x to 3.x:
- removed remote method framework completely (will be replaced by kontraktor actors on top of fast-cast)
- refurbished core NAK UDP streaming implementation. Renamed many classes to improve understandability
- simplified API significantly
- 3.0 has been optimized for low latency (2.x is a bastard latency wise ..). Depending on hardware/OS I have seen average latencys of <5 micro seconds. Detailed measurements (outliers+deviation) still open.
- requires fast-serialization 2.17 branch build for struct support
- allocation free under normal operation

build files broken currently (using IDE in dev).
