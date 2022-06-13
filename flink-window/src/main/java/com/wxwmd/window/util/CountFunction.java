package com.wxwmd.window.util;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author wxwmd
 * @description 在一个keyed stream中的window，去数这个window里面由多少个事件
 */
public class CountFunction extends ProcessWindowFunction<UserEvent, String, UserAction, TimeWindow> {

    @Override
    public void process(UserAction userAction, ProcessWindowFunction<UserEvent, String, UserAction, TimeWindow>.Context context,
                        Iterable<UserEvent> elements, Collector<String> out) {
        long startTime = context.window().getStart();
        long endTime = context.window().getEnd();
        long watermark = context.currentWatermark();

        int count = 0;
        for (UserEvent event: elements){
            count+=1;
        }
        String result = String.format("action: %s, window:[%d, %d], watermark: %d, count: %d",
                userAction.getAction(), startTime, endTime, watermark, count);
        out.collect(result);
    }
}
