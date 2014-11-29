package org.nustaq.fastcast.packeting;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 23.08.13
 * Time: 02:03
 * To change this template use File | Settings | File Templates.
 */
public class TopicStats implements Serializable {

    int packetsSent;
    int packetsReceived;
    int msgSent;
    int msgReceived;
    int retransRQSent;
    int retransRSPSent;
    int retransRQReceived;
    long recordStart;
    int lastSendPause;
    transient volatile TopicStats snapshot;
    long recordEnd;
    private double retransReq;
    private int dgramSiz;
    private long sendBytes;
    private long bytesReceived;

    public TopicStats(int dgramSiz) {
        recordStart = System.currentTimeMillis();
        this.dgramSiz = dgramSiz;
    }

    public TopicStats(long bytesReceived, long sendBytes, int packetsSent, int packetsReceived,
                      int msgSent, int msgReceived, int retransRQSent, int retransRSPSent, long recordStart, int rqRec, int dgramSiz) {
        this.packetsSent = packetsSent;
        this.sendBytes = sendBytes;
        this.packetsReceived = packetsReceived;
        this.msgSent = msgSent;
        this.bytesReceived = bytesReceived;
        this.msgReceived = msgReceived;
        this.retransRQSent = retransRQSent;
        this.retransRSPSent = retransRSPSent;
        this.recordStart = recordStart;
        this.retransRQReceived = rqRec;
        this.dgramSiz = dgramSiz;
    }

    public TopicStats snapshot() {
        return new TopicStats(bytesReceived, sendBytes, packetsSent,packetsReceived,msgSent,msgReceived,retransRQSent,retransRSPSent,recordStart,retransRQReceived,dgramSiz);
    }

    public double getMsgReceivedPerPacket() {
        if ( packetsReceived == 0 )
            return 0;
        return (double)msgReceived/packetsReceived;
    }

    public double getMsgReceived() {
        long dur = recordEnd - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)msgReceived*1000 / dur;
    }

    public double getMsgSentPerPacket() {
        if ( packetsSent == 0 )
            return 0;
        return (double)msgSent/packetsSent;
    }

    public double getPacketsSentPerSecond() {
        return getPacketsSentPerSecond(recordEnd);
    }

    public long getBytesSentPerSecond() {
        return getBytesSentPerSecond(recordEnd);
    }

    public long getBytesRecPerSecond() {
        return getBytesRecPerSecond(recordEnd);
    }

    public long getBytesSentPerSecond(long millis) {
        long dur = millis - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return sendBytes*1000 / dur;
    }

    public long getBytesRecPerSecond(long millis) {
        long dur = millis - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return bytesReceived*1000 / dur;
    }

    public double getPacketsSentPerSecond(long nowMillis) {
        long dur = nowMillis - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)packetsSent*1000 / dur;
    }

    public double getPacketsRetransSentPerSecond() {
        return getPacketsRetransSentPerSecond(recordEnd);
    }

    public double getPacketsRetransSentPerSecond(long nowMillis) {
        long dur = nowMillis - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)retransRSPSent*1000 / dur;
    }

    public double getRetransReq() {
        long dur = recordEnd - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)retransRQSent*1000 / dur;
    }

    public double getMsgSent() {
        long dur = recordEnd - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)msgSent*1000 / dur;
    }

    public double getPacketsRecPerSecond() {
        return getPacketsRecPerSecond(recordEnd);
    }

    public double getPacketsRecPerSecond(long nowMillis) {
        long dur = nowMillis - recordStart;
        if ( dur == 0 ) {
            return 0;
        }
        return (double)packetsReceived*1000 / dur;
    }

    public void reset() {
        synchronized (this) {
            long now = System.currentTimeMillis();
            if ( now - recordStart < 1000 ) {
                return;
            }
            snapshot = snapshot();
            snapshot.recordEnd = now;
            snapshot.lastSendPause = lastSendPause;
            snapshot.snapshot = null;
            this.packetsSent = 0;
            this.sendBytes = 0;
            this.bytesReceived = 0;
            this.packetsReceived = 0;
            this.msgSent = 0;
            this.msgReceived = 0;
            this.retransRQSent = 0;
            this.retransRSPSent = 0;
            this.retransRQReceived = 0;
            recordStart = now;
        }
    }

    public TopicStats getSnapshot() {
        return snapshot;
    }

    public void addTo(TopicStats other, int divisor) {
        other.packetsSent = (packetsSent + other.packetsSent * (divisor-1))/divisor;
        other.sendBytes = (sendBytes + other.sendBytes * (divisor-1))/divisor;
        other.bytesReceived = (bytesReceived + other.bytesReceived * (divisor-1))/divisor;
        other.packetsReceived = (packetsReceived + other.packetsReceived * (divisor-1))/divisor;
        other.msgSent = (msgSent + other.msgSent * (divisor-1))/divisor;
        other.msgReceived = (msgReceived + other.msgReceived * (divisor-1))/divisor;
        other.retransRQSent = (retransRQSent + other.retransRQSent * (divisor-1))/divisor;
        other.retransRQReceived = (retransRQReceived + other.retransRQReceived * (divisor-1))/divisor;
        other.retransRSPSent = (retransRSPSent + other.retransRSPSent * (divisor-1))/divisor;
    }

    public long getRecordStart() {
        return recordStart;
    }

    public long getRecordEnd() {
        return recordEnd;
    }

    public void dataPacketSent(int size) {
        packetsSent++;
        sendBytes += size;
    }

    public void dataPacketReceived(int size) {
        packetsReceived++;
        bytesReceived+=size;
    }

    public double getRetransVSDataPacketPercentage() {
        if (packetsSent < 1)
            return 0;
        return (double)retransRQReceived/packetsSent;
    }

    public int getLastSendPause() {
        return lastSendPause;
    }

    public void setLastSendPause(int lastSendPauseUncorrected) {
        this.lastSendPause = lastSendPauseUncorrected;
    }

    public void msgSent() {
        msgSent++;
    }

    public void msgReceived() {
        msgReceived++;
    }

    public void retransRQSent(int num) {
        retransRQSent +=num;
    }

    public void retransRQReceived(int num, int myRate) {
        retransRQReceived +=num;
    }

    public void retransRSPSent(int num, int size) {
        retransRSPSent +=num;
    }

    @Override
    public String toString() {
        long now = recordEnd == 0 ? System.currentTimeMillis() : recordEnd;
        return "TopicStats{\n" +
                "  packetsSent=" + packetsSent + "\n"+
                "  packetsReceived=" + packetsReceived +"\n"+
                "  msgSent=" + msgSent +"\n"+
                "  msgReceived=" + msgReceived +"\n"+
                "  retransRespSent=" + retransRSPSent +"\n"+
                "  retransRQSent=" + retransRQSent +"\n"+
                "  retransRQRec=" + retransRQReceived +"\n"+
                "  ---------------"+"\n"+
                "  msgRec/Pack =" + getMsgReceivedPerPacket() +"\n"+
                "  msgSent/Pack=" + getMsgSentPerPacket() +"\n"+
                "  packRec/s   =" + getPacketsRecPerSecond(now) +"\n"+
                "  packSent/s  =" + getPacketsSentPerSecond(now) +"\n"+
                "  retransPerc =" + getRetransVSDataPacketPercentage() +"\n"+
                "  flow pause  =" + lastSendPause +"\n"+
                '}';
    }

    public int getDgramSize() {
        return dgramSiz;
    }

}
