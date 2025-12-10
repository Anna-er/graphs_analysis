package org.example.mst;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class BoruvkaMasterCompute extends DefaultMasterCompute {

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerPersistentAggregator(BoruvkaMSTComputation.AGG_MST_WEIGHT, DoubleSumAggregator.class);
        registerPersistentAggregator(BoruvkaMSTComputation.AGG_ROOT_COUNT, LongSumAggregator.class);
        registerPersistentAggregator(BoruvkaMSTComputation.AGG_PARENT_CHANGES, LongSumAggregator.class);
        registerPersistentAggregator(BoruvkaMSTComputation.AGG_HOOKS, LongSumAggregator.class);

        setAggregatedValue(BoruvkaMSTComputation.AGG_MST_WEIGHT, new DoubleWritable(0.0));
        setAggregatedValue(BoruvkaMSTComputation.AGG_ROOT_COUNT, new LongWritable(0L));
        setAggregatedValue(BoruvkaMSTComputation.AGG_PARENT_CHANGES, new LongWritable(0L));
        setAggregatedValue(BoruvkaMSTComputation.AGG_HOOKS, new LongWritable(0L));
    }

    @Override
    public void compute() {
        long sid = getSuperstep();
        if (sid == 0) return;

        int phase = (int) (sid % 7);
        if (phase == 6) {
            long rootCount = ((LongWritable) getAggregatedValue(BoruvkaMSTComputation.AGG_ROOT_COUNT)).get();
            long hooks = ((LongWritable) getAggregatedValue(BoruvkaMSTComputation.AGG_HOOKS)).get();

            setAggregatedValue(BoruvkaMSTComputation.AGG_ROOT_COUNT, new LongWritable(0L));
            setAggregatedValue(BoruvkaMSTComputation.AGG_PARENT_CHANGES, new LongWritable(0L));
            setAggregatedValue(BoruvkaMSTComputation.AGG_HOOKS, new LongWritable(0L));

            if (rootCount == 1 || hooks == 0) {
                haltComputation();
            }
        }
    }

    public double getMstWeight() {
        return ((DoubleWritable) getAggregatedValue(BoruvkaMSTComputation.AGG_MST_WEIGHT)).get();
    }
}
