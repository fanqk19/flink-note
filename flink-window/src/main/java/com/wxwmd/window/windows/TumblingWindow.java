package com.wxwmd.window.windows;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static com.wxwmd.window.watermark.AssignWatermark.getFileStreamWithWatermark;

/**
 * @author wxwmd
 * @description 滚动窗口
 * 演示如何计算每个滚动窗口内有多少个登录事件
 */
public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 得到带有事件时间和watermark的source流
        DataStream<UserEvent> source = getFileStreamWithWatermark(env);

        DataStream<Object> loginCountStream = loginCount(source);

        /*
        这里想说一下，如果多次运行程序，会看到一个非常有趣的现象：
        window: [1654688060, 1654688070]对应的count一会是0，一会是1
        这是因为我们是分布式读取文件，如果事件时间很大的那些事件先被读取，那么在doris,LOGIN,1654688067被读取之前，
        这些事件时间很大的事件就把水位线设置为>1654688070，因此window: [1654688060, 1654688070]已经被关闭了，此时count是0
        如果doris,LOGIN,1654688067先被读取，之后才读取到那些事件时间很大的事件，此时关口关闭，计算count结果为1
         */
        loginCountStream.print("login count per tumbling window: ");

        env.execute();
    }

    /**
     * non-keyed stream的窗口操作
     * 滚动窗口，10ms开一个窗，统计在这个窗口内有多少人登录（LOGIN事件）
     * @param source 已经带有timestamp和watermark的数据流
     * @return 每个窗口内的登录人数
     */
    public static DataStream<Object> loginCount(DataStream<UserEvent> source){
//        AggregateFunction
//        SingleOutputStreamOperator loginCountStream = source.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10L)))
//                .aggregate(new LoginCountAggregateFunction());

//        ProcessFunction
        SingleOutputStreamOperator loginCountStream = source.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10L)))
                .process(new LoginCountProcessFunction());

        return loginCountStream;
    }

    /**
     * 使用ProcessFunction计算一个窗口内有多少登录事件
     */
    public static class LoginCountProcessFunction extends ProcessAllWindowFunction<UserEvent, String, TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<UserEvent, String, TimeWindow>.Context context, Iterable<UserEvent> elements, Collector<String> out) throws Exception {
            long startTime = context.window().getStart();
            long endTime = context.window().getEnd();

            int count = 0;
            for (UserEvent userEvent : elements) {
                if (userEvent.getAction().equals(UserAction.LOGIN)) {
                    count += 1;
                }
            }
            String loginCount = String.format("window: [%d, %d]: login count: %d", startTime, endTime, count);
            out.collect(loginCount);
        }
    }

    /**
     * 使用AggregateFunction计算一个窗口内有多少登录事件
     */
    public static class LoginCountAggregateFunction extends RichAggregateFunction<UserEvent, Integer, Integer> {

        /**
         * 创建累加器，初始值为0
         * @return 0
         */
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        /**
         * 当元素落入窗口中，对累加器的值进行更新
         * 我们这里进行了一个简单的判断，如果UserEvent是登录活动，那么累加器+1
         * @param value 落在窗口内的元素
         * @param accumulator old累加器
         * @return 累加器的新值
         */
        @Override
        public Integer add(UserEvent value, Integer accumulator) {
            if (value.getAction().equals(UserAction.LOGIN)){
                return accumulator+1;
            }
            return accumulator;
        }

        /**
         * 窗口被触发时，AggregateFunction返回的计算结果
         * @param accumulator 累加器
         * @return 窗口的计算结果
         */
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


}
