package org.example.mst;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.example.mst.messages.MSTMessage;
import org.apache.giraph.edge.Edge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class BoruvkaMSTComputation extends BasicComputation<
        LongWritable, LongWritable, DoubleWritable, MSTMessage> {

    public static final String AGG_MST_WEIGHT = "agg_mst_weight";
    public static final String AGG_ROOT_COUNT = "agg_root_count";
    public static final String AGG_PARENT_CHANGES = "agg_parent_changes";
    public static final String AGG_HOOKS = "agg_hooks";

    @Override
    public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex, Iterable<MSTMessage> messages) throws IOException {
        long sid = getSuperstep();

        if (sid == 0) {
            vertex.setValue(new LongWritable(vertex.getId().get()));
            return;
        }

        int phase = (int) (sid % 7);

        if (phase == 1) {
            long root = vertex.getValue().get();
            sendMessageToAllEdges(vertex, MSTMessage.announce(vertex.getId().get(), root));

        } else if (phase == 2) {
            long myRoot = vertex.getValue().get();

            Map<Long, Long> nbrRoot = new HashMap<>();
            for (MSTMessage m : messages) {
                if (m.type == 0) {
                    nbrRoot.put(m.srcVertexId, m.srcRootId);
                }
            }

            double bestW = Double.POSITIVE_INFINITY;
            long bestOtherRoot = -1;
            long tieA = 0, tieB = 0;

            for (Edge<LongWritable, DoubleWritable> e : vertex.getEdges()) {
                long nbr = e.getTargetVertexId().get();
                Long r = nbrRoot.get(nbr);
                if (r == null || r == myRoot) continue;

                double w = e.getValue().get();
                long a = Math.min(vertex.getId().get(), nbr);
                long b = Math.max(vertex.getId().get(), nbr);

                if (w < bestW || (w == bestW && (a < tieA || (a == tieA && b < tieB)))) {
                    bestW = w;
                    bestOtherRoot = r;
                    tieA = a; tieB = b;
                }
            }

            if (bestOtherRoot != -1) {
                sendMessage(new LongWritable(myRoot),
                        MSTMessage.candidate(myRoot, bestOtherRoot, bestW, tieA, tieB));
            }

        } else if (phase == 3) {
            long myId = vertex.getId().get();
            long parent = vertex.getValue().get();

            if (parent == myId) {
                double bestW = Double.POSITIVE_INFINITY;
                long bestOtherRoot = -1;
                long bestA = 0, bestB = 0;

                for (MSTMessage m : messages) {
                    if (m.type != 1) continue;
                    if (m.srcRootId != myId) continue;
                    double w = m.edgeWeight;
                    if (w < bestW || (w == bestW && (m.tieA < bestA || (m.tieA == bestA && m.tieB < bestB)))) {
                        bestW = w;
                        bestOtherRoot = m.targetRootId;
                        bestA = m.tieA; bestB = m.tieB;
                    }
                }

                if (bestOtherRoot != -1) {
                    if (myId > bestOtherRoot) {
                        vertex.setValue(new LongWritable(bestOtherRoot));
                        aggregate(AGG_MST_WEIGHT, new DoubleWritable(bestW));
                        aggregate(AGG_HOOKS, new LongWritable(1L));
                        sendMessage(new LongWritable(bestOtherRoot), MSTMessage.hook(myId, bestOtherRoot, bestW));
                    }
                }
            }

        } else if (phase == 4) {
            long parent = vertex.getValue().get();
            sendMessage(new LongWritable(parent), MSTMessage.parentQuery(vertex.getId().get()));

        } else if (phase == 5) {
            long myParent = vertex.getValue().get();
            for (MSTMessage m : messages) {
                if (m.type != 3) continue;
                sendMessage(new LongWritable(m.srcVertexId), MSTMessage.parentReply(m.srcVertexId, myParent));
            }

        } else if (phase == 6) {
            long oldParent = vertex.getValue().get();
            long newParent = oldParent;

            for (MSTMessage m : messages) {
                if (m.type != 4) continue;
                newParent = m.targetRootId;
            }

            if (newParent != oldParent) {
                vertex.setValue(new LongWritable(newParent));
                aggregate(AGG_PARENT_CHANGES, new LongWritable(1L));
            }

            if (vertex.getValue().get() == vertex.getId().get()) {
                aggregate(AGG_ROOT_COUNT, new LongWritable(1L));
            }
        }

    }

    @Override
    public void preSuperstep() {
        GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(getConf(), false);
    }
}
