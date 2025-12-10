package org.example.mst.messages;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MSTMessage implements Writable {
    public byte type;

    public long srcVertexId;
    public long srcRootId;
    public long targetRootId;
    public double edgeWeight;

    public long tieA;
    public long tieB;

    public MSTMessage() {}

    public static MSTMessage announce(long srcVertexId, long srcRootId) {
        MSTMessage m = new MSTMessage();
        m.type = 0;
        m.srcVertexId = srcVertexId;
        m.srcRootId = srcRootId;
        return m;
    }

    public static MSTMessage candidate(long srcRootId, long targetRootId, double w, long a, long b) {
        MSTMessage m = new MSTMessage();
        m.type = 1;
        m.srcRootId = srcRootId;
        m.targetRootId = targetRootId;
        m.edgeWeight = w;
        m.tieA = a;
        m.tieB = b;
        return m;
    }

    public static MSTMessage hook(long higherRoot, long lowerRoot, double w) {
        MSTMessage m = new MSTMessage();
        m.type = 2;
        m.srcRootId = higherRoot;
        m.targetRootId = lowerRoot;
        m.edgeWeight = w;
        return m;
    }

    public static MSTMessage parentQuery(long childVertexId) {
        MSTMessage m = new MSTMessage();
        m.type = 3;
        m.srcVertexId = childVertexId;
        return m;
    }

    public static MSTMessage parentReply(long childVertexId, long parentsParentId) {
        MSTMessage m = new MSTMessage();
        m.type = 4;
        m.srcVertexId = childVertexId;
        m.targetRootId = parentsParentId;
        return m;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type);
        out.writeLong(srcVertexId);
        out.writeLong(srcRootId);
        out.writeLong(targetRootId);
        out.writeDouble(edgeWeight);
        out.writeLong(tieA);
        out.writeLong(tieB);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readByte();
        srcVertexId = in.readLong();
        srcRootId = in.readLong();
        targetRootId = in.readLong();
        edgeWeight = in.readDouble();
        tieA = in.readLong();
        tieB = in.readLong();
    }
}