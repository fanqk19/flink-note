package com.wxwmd.window.windows.keyed;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static com.wxwmd.window.watermark.AssignWatermark.getFileStreamWithWatermark;
import static com.wxwmd.window.watermark.AssignWatermark.getSocketStreamWithWatermark;
import static com.wxwmd.window.watermark.SourceWithWatermark.getKafkaSourceWithWatermark;


/**
 * @author wxwmd
 * @description keyed stream滚动窗口
 * 先将事件流 keyby(事件类型) 得到{LOGIN,BUY,LOGOUT}三个keyed stream
 * 再开窗计算每20ms内每个事件的发生个数
 *
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
