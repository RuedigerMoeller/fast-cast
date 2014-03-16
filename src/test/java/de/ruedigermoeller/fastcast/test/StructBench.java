package de.ruedigermoeller.fastcast.test;

import de.ruedigermoeller.fastcast.packeting.TopicStats;
import de.ruedigermoeller.fastcast.remoting.*;
import de.ruedigermoeller.fastcast.util.FCUtils;
import de.ruedigermoeller.heapoff.bytez.Bytez;
import de.ruedigermoeller.heapoff.bytez.onheap.HeapBytez;
import de.ruedigermoeller.heapoff.structs.FSTStruct;
import de.ruedigermoeller.heapoff.structs.FSTStructAllocator;
import de.ruedigermoeller.heapoff.structs.structtypes.StructString;
import de.ruedigermoeller.heapoff.structs.unsafeimpl.FSTStructFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 10/3/13
 * Time: 3:32 PM
 * To change this template use File | Settings | File Templates.
 */
@DecodeInTransportThread(true)
public class StructBench extends FCTopicService {

    // create single processeor thread
    Executor processor = (ExecutorService) FCUtils.createBoundedSingleThreadExecutor("structbend", 10000);
    boolean useProcessorThread = false;
    static int receiveCount;
    static int filterCount;
    static long lastTime;
    private char assignedChar = 'A';

    public static class StructMessage extends FSTStruct {

        protected StructString instrument = new StructString(4);
        protected double price;
        protected int quantity;
        protected long time;

        public StructString getInstrument() {
            return instrument;
        }

        public void setInstrument(StructString instrument) {
            this.instrument = instrument;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }


    @RemoteHelper
    public void sendStructMessage( FSTStruct offheaped ) {
        if ( ! offheaped.isOffHeap() )
            throw new RuntimeException("Expect offheaped struct");
        receiveBinary(offheaped.getBase(), (int) offheaped.getOffset(),offheaped.getByteSize());
    }

    @Override
    @RemoteMethod(-1) // internal flag, do not use negative indizes in your code
    public void receiveBinary(Bytez bytes, int offset, int length) {
        StructMessage volatileStructPointer = (StructMessage) FSTStructFactory.getInstance().getStructPointer(bytes, offset);
        if ( filter(volatileStructPointer) ) {
            if (useProcessorThread) {
                // that's why multithreaded processing frequently does not break even
                final StructMessage copy = (StructMessage) volatileStructPointer.createCopy();
                processor.execute( new Runnable() {
                    @Override
                    public void run() {
                        processMessage(copy);
                    }
                });
            } else {
                // if decodeInTransportThread == true => hurry up here, else packet losses might happen frequently
                processMessage(volatileStructPointer);
            }
        }
    }

    @RemoteMethod(1)
    public void assignStartChar(char c) {
        assignedChar = c;
        System.out.println("got assigned '"+c+"'");
    }

    @RemoteMethod(2)
    public void bitteMeldeDich(FCFutureResultHandler res) {
        res.sendResult("nothing");
    }

    private void processMessage(StructMessage copy) {
        receiveCount++;
        long l = System.currentTimeMillis();
//        System.out.println("rec "+copy.getInstrument()+" "+copy.getQuantity());
        if (l - lastTime > 1000) {
            synchronized (getClass()) {
                if (l - lastTime > 1000) {
                    System.out.println("received/s " + receiveCount+" filtered "+filterCount);
                    receiveCount = 0;
                    filterCount = 0;
                    lastTime = System.currentTimeMillis();
                    TopicStats stats = getRemoting().getStats(getTopicName());
                }
            }
        }
    }

    /**
     * @param volatileStructPointer
     * @return true if message should get processed
     */
    private boolean filter(StructMessage volatileStructPointer) {
        boolean res = volatileStructPointer.getInstrument().chars(0) == assignedChar;
        if ( ! res ) {
            filterCount++;
        }
        return res;
    }

    public static void main(String arg[]) throws IOException, InterruptedException {

        // NEVER FORGET WHEN REMOTING STRUCTS
        // NEVER FORGET WHEN REMOTING STRUCTS
        // NEVER FORGET WHEN REMOTING STRUCTS
        FSTStructFactory.getInstance().registerClz(StructMessage.class);
        // NEVER FORGET WHEN REMOTING STRUCTS
        // NEVER FORGET WHEN REMOTING STRUCTS
        // NEVER FORGET WHEN REMOTING STRUCTS

        FCRemoting fc = FastCast.getRemoting();
        fc.joinCluster("structbench.yaml", "Bench", null);
        if ( arg.length == 1 ) // => sender
        {
            System.out.println("Running in Sender mode ..");
            fc.startSending("structbench");
            final StructBench remote = (StructBench) fc.getRemoteService("structbench");

            // assign each listener a filter first char
            final AtomicInteger ch = new AtomicInteger('A');
            remote.bitteMeldeDich(new FCFutureResultHandler() {
                @Override
                public void resultReceived(Object obj, String sender) {
                    FCSendContext.get().setReceiver(sender);
                    char newChar = (char) ch.getAndIncrement();
                    remote.assignStartChar(newChar);
                    System.out.println("assigned "+sender+" char "+newChar);
                }
            });

            FSTStructAllocator alloc = new FSTStructAllocator(50000);
            StructMessage msgTemplate = (StructMessage) alloc.newStruct(new StructMessage()).detach();
            int count = 0;
            while( true ) {
                int rand = (int) (Math.random()*10);
                msgTemplate.getInstrument().setString( ((char)('A'+rand))+"LV");
                msgTemplate.setPrice(Math.random()*100);
                msgTemplate.setQuantity((int) (Math.random()*1000));
                msgTemplate.setTime(System.currentTimeMillis());

//                System.out.println("send binary "+msgTemplate.getBase()+" "+msgTemplate.getOffset()+" "+msgTemplate.getByteSize());
//                remote.receiveBinary(msgTemplate.getBase(),msgTemplate.getOffset(),msgTemplate.getByteSize());
                remote.sendStructMessage(msgTemplate);
//                if ( rand == 0 )
//                    Thread.sleep(1);
            }
        } else {
            System.out.println("Running in Receiver mode ..");
            fc.start("structbench");
        }
    }

}

