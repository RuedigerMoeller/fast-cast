package de.ruedigermoeller.fastcast.control;

import de.ruedigermoeller.fastcast.packeting.*;
import de.ruedigermoeller.fastcast.remoting.FCRemotingListener;
import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.FastCast;
import de.ruedigermoeller.fastcast.transport.Transport;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.heapoff.structs.FSTStructAllocator;
import de.ruedigermoeller.heapoff.structs.structtypes.StructString;
import de.ruedigermoeller.kontraktor.Actor;
import de.ruedigermoeller.kontraktor.impl.DispatcherThread;

import java.io.IOException;
import java.net.DatagramPacket;

/**
* Created by ruedi on 13.05.14.
 *
 * Polls a transport (~socket) and dispatches incoming msg to different top listeners.
*/
public class TransportReceiveActor extends Actor
{
    // assumed constant
    StructString clusterName;
    StructString nodeId;

    // shared !!
    Transport trans;
    ReceiveBufferDispatcher receiver[]; // fixme. can mutate (slow, uncritical). make them actors
    PacketSendBuffer sender[];          // fixme. can mutate (slow, uncritical). Only use: detect if there is a sender on a given topic

    // local
    FSTStructAllocator alloc = new FSTStructAllocator(1);
    Packet receivedPacket;
    byte[] receiveBuf;
    DatagramPacket packet;
    int idleCount;

    public void init( Transport transport, StructString clusterName, StructString nodeId, ReceiveBufferDispatcher receiver[], PacketSendBuffer sender[] ) {
        this.trans = transport;
        receiveBuf = new byte[trans.getConf().getDgramsize()];
        packet = new DatagramPacket(receiveBuf,receiveBuf.length);
        receivedPacket = alloc.newStruct(new Packet());
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        idleCount = 0;
        this.receiver = receiver;
        this.sender = sender;
    }

    public void receiveLoop()
    {
        try {
            // FIXME: if loopback packets => loop spins as traffic is always avaiable
            if (receiveDatagram(packet)) {
                idleCount = 0;
            } else {
                idleCount++;
                //getDispatcher()
                DispatcherThread.yield(idleCount);
            }
        } catch (IOException e) {
            FCLog.log(e);
        }
        self().receiveLoop();
    }

    @Override
    protected TransportReceiveActor self() {
        return super.self();
    }

    private boolean receiveDatagram(DatagramPacket p) throws IOException {
        if ( trans.receive(p) ) {
            receivedPacket.baseOn(p.getData(), p.getOffset());

            boolean sameCluster = receivedPacket.getCluster().equals(clusterName);
            boolean selfSent = receivedPacket.getSender().equals(nodeId);

            if ( sameCluster && ! selfSent) {

                int topic = receivedPacket.getTopic();

                if ( topic > FCTransportDispatcher.MAX_NUM_TOPICS || topic < 0 ) {
                    FCLog.get().warn("foreign traffic");
                    return true;
                }
                if ( receiver[topic] == null && sender[topic] == null) {
                    return true;
                }

                Class type = receivedPacket.getPointedClass();
                StructString receivedPacketReceiver = receivedPacket.getReceiver();
                if ( type == DataPacket.class )
                {
                    if ( receiver[topic] == null )
                        return true;
                    if (
                            ( receivedPacketReceiver == null || receivedPacketReceiver.getLen() == 0 ) ||
                                    ( receivedPacketReceiver.equals(nodeId) )
                            )
                    {
                        dispatchDataPacket(receivedPacket, topic);
                    }
                } else if ( type == RetransPacket.class )
                {
                    if ( sender[topic] == null )
                        return true;
                    if ( receivedPacketReceiver.equals(nodeId) ) {
                        dispatchRetransmissionRequest(receivedPacket, topic);
                    }
                } else if (type == ControlPacket.class )
                {
                    ControlPacket control = (ControlPacket) receivedPacket.cast();
                    if ( control.getType() == ControlPacket.DROPPED &&
                            receivedPacketReceiver.equals(nodeId) ) {
                        ReceiveBufferDispatcher receiveBufferDispatcher = receiver[topic];
                        if ( receiveBufferDispatcher != null ) {
                            FCLog.get().warn(nodeId+" has been dropped by "+receivedPacket.getSender()+" on service "+receiveBufferDispatcher.getTopicEntry().getName());
                            FCTopicService service = receiveBufferDispatcher.getTopicEntry().getService();
                            if ( service != null ) {
                                service.droppedFromReceiving();
                            }
                            FCRemotingListener remotingListener = FastCast.getRemoting().getRemotingListener();
                            if ( remotingListener != null ) {
                                remotingListener.droppedFromTopic(receiveBufferDispatcher.getTopicEntry().getTopic(),receiveBufferDispatcher.getTopicEntry().getName());
                            }
                            receiver[topic] = null;
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

    private void dispatchRetransmissionRequest(Packet receivedPacket, int topic) throws IOException {
        RetransPacket retransPacket = (RetransPacket) receivedPacket.cast().detach();
        sender[topic].addRetransmissionRequest(retransPacket, trans);
    }

    private void dispatchDataPacket(Packet receivedPacket, int topic) throws IOException {
        PacketReceiveBuffer buffer = receiver[topic].getBuffer(receivedPacket.getSender());
        DataPacket p = (DataPacket) receivedPacket.cast().detach();
        RetransPacket retransPacket = buffer.receivePacket(p);
        if ( retransPacket != null ) {
            // packet is valid just in this thread
            if ( PacketSendBuffer.RETRANSDEBUG )
                System.out.println("send retrans request " + retransPacket + " " + retransPacket.getClzId());
            trans.send(new DatagramPacket(retransPacket.getBase().toBytes((int) retransPacket.getOffset(), retransPacket.getByteSize()), 0, retransPacket.getByteSize()));
        }
    }
}
