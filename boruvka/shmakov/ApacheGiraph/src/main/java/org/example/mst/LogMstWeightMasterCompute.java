package org.example.mst;

public class LogMstWeightMasterCompute extends BoruvkaMasterCompute {
    @Override
    public void compute() {
        super.compute(); 

        if (isHalted()) {
            double w = getMstWeight();
            System.out.printf("MST_WEIGHT=%.6f%n", w);
        }
    }
}
