package com.lfw.tolerance.twopc;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * An MapFunction that forwards all records.
 * <p>
 * Any instance of the function will fail after forwarding a configured number of records by
 * throwing an exception.
 * <p>
 * NOTE: This function is only used to demonstrate Flink's failure recovery capabilities.
 * <p>
 * failInterval: The number of records that are forwarded before throwing an exception.
 * IN: The type of input and output records.
 */
public class FailingMapper<IN> implements MapFunction<IN, IN> {

    private int cnt;

    private final int failInterval;

    public FailingMapper(int failInterval) {
        cnt = 0;
        this.failInterval = failInterval;
    }

    @Override
    public IN map(IN value) throws Exception {
        cnt += 1;

        //check failure condition
        if (cnt > failInterval) {
            throw new RuntimeException("Fail application to demonstrate output consistency.");
        }

        return value;
    }
}
