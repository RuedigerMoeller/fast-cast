package org.nustaq.fastcast.examples.latency;

import org.HdrHistogram.Histogram;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * Copied from peter lawreys blog + added HdrHistogram + emulation of echoing a 40 byte msg.
 *
 * Results are a good indication on how "quiet" a machine is latency wise
 *
 */
public class ShareMemPingPong {

public static void main(String... args) throws IOException {
        boolean odd;
        switch (args.length < 1 ? "usage" : args[0].toLowerCase()) {
            case "odd":
                odd = true;
                break;
            case "even":
                odd = false;
                break;
            default:
                System.err.println("Usage: java PingPongMain [odd|even]");
                return;
        }
        System.out.println("odd:"+odd);
        int runs = 10_000_000;
        long start = 0;
        System.out.println("Waiting for the other odd/even");
        File counters = new File(System.getProperty("java.io.tmpdir"), "counters.deleteme");
        counters.deleteOnExit();

        oneRun(odd, runs, start, counters);
        oneRun(odd, runs, start, counters);
    }

    public static void oneRun(boolean odd, int runs, long start, File counters) throws IOException {
        Histogram histo = new Histogram(TimeUnit.SECONDS.toNanos(10),3);
        byte msg[] = new byte[40];
        try (FileChannel fc = new RandomAccessFile(counters, "rw").getChannel()) {
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            long address = ((DirectBuffer) mbb).address();
            for (int i = -1; i < runs; i++) {
                for (; ; ) {
                    long tim = System.nanoTime();
                    long value = UNSAFE.getLongVolatile(null, address);
                    boolean isOdd = (value & 1) != 0;
                    if (isOdd != odd)
                        // wait for the other side.
                        continue;

                    // simulate reading message
                    mbb.position(8);
                    mbb.get(msg,0,msg.length);

                    // simulate writing msg
                    msg[12] = (byte) i;
                    mbb.position(8);
                    mbb.put(msg, 0, msg.length);

                    histo.recordValue(System.nanoTime()-tim);
                    // make the change atomic, just in case there is more than one odd/even process
                    if (UNSAFE.compareAndSwapLong(null, address, value, value + 1))
                        break;
                }
                if (i == 0) {
                    System.out.println("Started");
                    start = System.nanoTime();
                }
            }
        }
        System.out.printf("... Finished, average ping/pong took %,d ns%n",
                (System.nanoTime() - start) / runs);
        histo.outputPercentileDistribution(System.out, 1000.0);
    }

    static final Unsafe UNSAFE;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
