fast-cast
=========

High performance topic based reliable brokerless UDP messaging ("event-bus") backed by 1:N remote-method metaphor. Up to **12 million** remote calls per **second**. Buffers required to implement NAK protocol are kept offheap, so there is little impact on GC.
(just moved from gcode, some docs still missing)

**download** non-maven release (fat jar):

https://github.com/RuedigerMoeller/fast-cast/releases

**maven:**
```xml
<dependency>
    <groupId>de.ruedigermoeller</groupId>
    <artifactId>fast-cast</artifactId>
    <version>2.11</version>
</dependency>
```

Simple Sender/Receiver **example**

https://github.com/RuedigerMoeller/fast-cast/tree/master/src/test/java/de/ruedigermoeller/fastcast/samples/topicservice

**Documentation:**

https://github.com/RuedigerMoeller/fast-cast/wiki/Documentation

