some samples.

main classes expect to be run from within the examples directory (some require to locate config files).

Latency test:
=============

run FCPongServer, FCPing for synchronous RTT round trip measurement. The asynchronous FCPingClientAsync can be faulty
 because nanoTime is grabbed from different threads (=cores, cpu-sockets).

all tests default to localhost. Attention: on some OS'es/OS configurations localhost can perform particulary bad
(worse than a network card). E.g. clean CentOS 7 install localhost has like 400 micros (??). Ubuntu 14.04 LTS had ~20.
Pimped scientific Linux (RH 6.x based) had 10..12 micros RTT on localhost.

Best is to test on real ll network hardware.

Throughput Test
===============

On older cpu's or "Bad localhost"-OS'es, throughput test might disconnect receiver. Lower packet rate limit then
(see topic configuration).