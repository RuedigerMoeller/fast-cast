/*
 * Copyright (c) 2011.  Peter Lawrey
 *
 * "THE BEER-WARE LICENSE" (Revision 128)
 * As long as you retain this notice you can do whatever you want with this stuff.
 * If we meet some day, and you think this stuff is worth it, you can buy me a beer in return
 * There is no warranty.
 */

package stuff;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class PingTest {

//    public void testSocketWriteReadThroughput() throws IOException, InterruptedException {
//        final ServerSocketChannel ssc = ServerSocketChannel.open();
//        ssc.socket().bind(new InetSocketAddress("localhost", 9999));
//
//        SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 9999));
//        configure(sc);
//        SocketChannel sc2 = ssc.accept();
//        configure(sc2);
//        close(ssc);
//
//        ByteBuffer bb = ByteBuffer.allocateDirect(4096);
//        ByteBuffer bb2 = ByteBuffer.allocateDirect(4096);
//
//        long start = System.nanoTime();
//        int runs = 1000 * 1000;
//        for (int i = 0; i < runs; i++) {
//            bb.position(0);
//            bb.limit(8000);
//            sc.write(bb);
//
//            bb2.clear();
//            sc2.read(bb2);
//            bb2.flip();
//            sc2.write(bb2);
//
//            bb.clear();
//            sc.read(bb);
//        }
//        long time = System.nanoTime() - start;
//
//        close(sc);
//        close(sc2);
//        System.out.printf("Socket Throughput was %,d K/s%n", runs * 1000000L / time);
//    }
//
//    public void testSocketLatency() throws IOException, InterruptedException {
//        final ServerSocketChannel ssc = ServerSocketChannel.open();
//        ssc.socket().bind(new InetSocketAddress("localhost", 9999));
//        Thread server = new Thread(new Runnable() {
//            public void run() {
//                SocketChannel sc2 = null;
//                try {
//                    sc2 = ssc.accept();
//                    configure(sc2);
//                    sc2.configureBlocking(false);
//                    close(ssc);
//                    ByteBuffer bb2 = ByteBuffer.allocateDirect(2*4096);
//                    while (!Thread.interrupted()) {
//                        bb2.clear();
//                        do {
//                            sc2.read(bb2);
//                        } while (bb2.position() == 0);
//                        bb2.flip();
//                        sc2.write(bb2);
//                    }
//                } catch (ClosedByInterruptException ignored) {
//                } catch (ClosedChannelException ignored) {
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } finally {
//                    close(sc2);
//                }
//            }
//        });
//        server.start();
//        SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 9999));
//        configure(sc);
//        ByteBuffer bb = ByteBuffer.allocateDirect(2*4096);
//        long times[] = new long[1000 * 1000];
//        for (int i = -10000; i < times.length; i++) {
//            long start = System.nanoTime();
//            bb.position(0);
//            bb.limit(32);
//            sc.write(bb);
//
//
//            bb.clear();
//            sc.read(bb);
//            long end = System.nanoTime();
//            long err = 0;//System.nanoTime() - end;
//            long time = end - start - err;
//            if (i >= 0)
//                times[i] = time;
//        }
//        server.interrupt();
//        close(sc);
//        Arrays.sort(times);
//        System.out.printf("Threaded Socket Latency for 1/50/99%%tile %.1f/%.1f/%.1f us%n",
//                times[times.length / 100] / 1e3,
//                times[times.length / 2] / 1e3,
//                times[times.length - times.length / 100 - 1] / 1e3
//        );
//    }
//
//    public void testSocketThreadedThroughput() throws IOException, InterruptedException {
//        final ServerSocketChannel ssc = ServerSocketChannel.open();
//        ssc.socket().bind(new InetSocketAddress("localhost", 9999));
//
//        Thread server = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                SocketChannel sc2 = null;
//                try {
//                    sc2 = ssc.accept();
//                    configure(sc2);
//                    close(ssc);
//                    ByteBuffer bb2 = ByteBuffer.allocateDirect(4096);
//                    while (!Thread.interrupted()) {
//                        bb2.clear();
//                        sc2.read(bb2);
//                        bb2.flip();
//                        sc2.write(bb2);
//                    }
//                } catch (ClosedByInterruptException ignored) {
//                } catch (ClosedChannelException ignored) {
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } finally {
//                    close(sc2);
//                }
//            }
//        });
//        server.start();
//        SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 9999));
//        configure(sc);
//
//        ByteBuffer bb = ByteBuffer.allocateDirect(4096);
//
//        long start = System.nanoTime();
//        int runs = 1000 * 1000;
//        for (int i = 0; i < runs; i += 2) {
//            bb.position(0);
//            bb.limit(1024);
//            sc.write(bb);
//
//            bb.position(0);
//            bb.limit(1024);
//            sc.write(bb);
//
//            bb.clear();
//            sc.read(bb);
//        }
//        server.interrupt();
//        server.join();
//        long time = System.nanoTime() - start;
//        close(sc);
//        System.out.printf("Threaded Socket Throughput was %,d K/s%n", runs * 1000000L / time);
//    }
//
//    static void close(Closeable sc2) {
//        if (sc2 != null) try {
//            sc2.close();
//        } catch (IOException ignored) {
//        }
//    }
//
//
//    static void configure(SocketChannel sc) throws SocketException {
//        sc.socket().setTcpNoDelay(true);
//    }
//
//    public static void main(String arg[]) throws IOException, InterruptedException {
//        new PingTest().testSocketLatency();
//    }
}
