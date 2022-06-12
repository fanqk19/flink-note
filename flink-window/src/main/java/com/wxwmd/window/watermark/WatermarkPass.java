package com.wxwmd.window.watermark;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import static com.wxwmd.window.watermark.AssignWatermark.getSocketStreamWithWatermark;

/**
 * @author wxwmd
 * @description 展示watermark从上游向下游传递的现象
 * 代码的详细讲解： https://blog.csdn.net/cobracanary/article/details/125237966
 *
 * 使用方法：
 * nc启动12345端口后
 *
 * 输入：
 * bob,LOGIN,1654688058
 * burg,LOGIN,1654688058
 * bob,BUY,1654688064
 * doris,LOGIN,1654688081
 * burg,BUY,1654688084
 * doris,LOGOUT,1654688089
 * wxwmd,LOGIN,1654688100
 */
public class WatermarkPass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<UserEvent> source = getSocketStreamWithWatermark(env, "localhost", 12345);

        DataStream<String> watermarks = getWatermark(source);

        watermarks.print("watermark infos");

        env.execute();
    }


    static DataStream<String> getWatermark(DataStream<UserEvent> source){
        SingleOutputStreamOperator<String> watermarks = source.keyBy(UserEvent::getUserAction) // 先进行一次shuffle，从上游的三个source算子到下游的
                .process(new GetWatermarkProcessFunction());

        return watermarks;
    }

    static class GetWatermarkProcessFunction extends KeyedProcessFunction<UserAction, UserEvent, String> {
        @Override
        public void processElement(UserEvent userEvent, KeyedProcessFunction<UserAction, UserEvent, String>.Context ctx, Collector<String> out) {
            long watermark = ctx.timerService().currentWatermark();
            String result = String.format("user event: %s, watermark: %d", userEvent.toString(), watermark);
            out.collect(result);
        }
    }
}
