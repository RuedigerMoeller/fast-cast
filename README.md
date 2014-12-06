fast-cast
=========


High performance low latency topic/stream based reliable UDP messaging ("event-bus").

3.x is in the making, see Wiki for documentation for old 2.x release. 2.x is avaialable at maven.

Changes done from 2.x to 3.x:
- removed remote method framework completely (will be replaced by kontraktor actor on top of fast-cast)
- refurbished core NAK UDP streaming implementation. Renamed many classes to improve understandability
- simplified API significantly
- optimized for low latency. Depending on hardware/OS I have seen average latencys of <5 micro seconds. Detailed measurements (outliers+deviation) still open.
- requires fast-serialization 2.17 branch build for struct support

Use mvn build, the gradle build is for private use and might not work.
