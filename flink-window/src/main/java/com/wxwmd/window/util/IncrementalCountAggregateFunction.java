package com.wxwmd.window.util;

import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author wxwmd
 * @description
 */
public class IncrementalCountAggregateFunction implements AggregateFunction<UserEvent, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(UserEvent value, Integer accumulator) {
        return accumulator+1;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
