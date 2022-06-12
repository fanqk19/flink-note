package com.wxwmd.window.windows.keyed;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static com.wxwmd.window.watermark.AssignWatermark.getFileStreamWithWatermark;
import static com.wxwmd.window.watermark.AssignWatermark.getSocketStreamWithWatermark;


/**
 * @author wxwmd
 * @description keyed stream滚动窗口
 * 先将事件流 keyby(事件类型) 得到{LOGIN,BUY,LOGOUT}三个keyed stream
 * 再开窗计算每20ms内每个事件的发生个数
 * 代码的详细讲解：https://blog.csdn.net/cobracanary/article/details/125234192
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

        //1. 使用socket作为data source
        DataStream<UserEvent> source = getSocketStreamWithWatermark(env, "localhost", 12345);
        //2.  使用本地文件作为data source
        //DataStream<UserEvent> source = getFileStreamWithWatermark(env);
        DataStream<String> countStream = countEventByKeyedWindow(source);
        countStream.print("event count ");

        env.execute();
    }


    /**
     * 先按事件类型{LOGIN,BUY,LOGOUT}进行分区，然后对keyed stream进行开窗计算
     * @param source 事件流
     * @return 结果流
     */
    static DataStream<String> countEventByKeyedWindow(DataStream<UserEvent> source) {
        SingleOutputStreamOperator<String> countStream = source.keyBy(UserEvent::getUserAction)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10L)))
                .process(new CountEventProcessWindow());

        return countStream;
    }

    /**
     * 在一个keyed stream中的window，去数这个window里面由多少个事件
     */
    static class CountEventProcessWindow extends ProcessWindowFunction<UserEvent, String, UserAction, TimeWindow>{

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
}
