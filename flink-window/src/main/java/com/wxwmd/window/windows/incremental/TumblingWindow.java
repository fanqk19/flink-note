package com.wxwmd.window.windows.incremental;

import com.wxwmd.util.model.UserEvent;
import com.wxwmd.window.util.IncrementalCountAggregateFunction;
import com.wxwmd.window.util.IncrementalCountFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.wxwmd.window.watermark.AssignWatermark.getSocketStreamWithWatermark;


/**
 * @author wxwmd
 * @description keyed stream滚动窗口
 * 与AggregateFunction结合，实现ProcessFunction增量计算
 *
 * 使用方法：
 * 打开端口
 * 在端口中逐次输入：
 * bob,LOGIN,1654688058
 * burg,LOGIN,1654688058
 * bob,BUY,1654688064
 * doris,LOGIN,1654688081
 * burg,BUY,1654688084
 * doris,LOGOUT,1654688089
 * wxwmd,LOGIN,1654688100
 * duck,LOGIN,1654688096
 * note：这是窗口聚合计算，所以不是输入一条就输出一条的
 *
 */
public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //使用socket作为data source
        DataStream<UserEvent> source = getSocketStreamWithWatermark(env, "localhost", 12345);

        DataStream<String> countStream = countEventByTumblingWindow(source);
        countStream.print("event count ");

        env.execute();
    }


    /**
     * 先按事件类型{LOGIN,BUY,LOGOUT}进行分区，然后对keyed stream进行开窗计算
     * @param source 事件流
     * @return 结果流
     */
    static DataStream<String> countEventByTumblingWindow(DataStream<UserEvent> source) {
        SingleOutputStreamOperator<String> countStream = source.keyBy(UserEvent::getUserAction)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10L)))
                .aggregate(new IncrementalCountAggregateFunction(), new IncrementalCountFunction());

        return countStream;
    }

}
