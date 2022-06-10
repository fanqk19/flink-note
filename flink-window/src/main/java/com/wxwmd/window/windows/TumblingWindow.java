package com.wxwmd.window.windows;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.wxwmd.window.watermark.AssignWatermark.getFileStreamWithWatermark;

/**
 * @author wxwmd
 * @description 滚动窗口
 */
public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 得到带有事件时间和watermark的source流
        DataStream<UserEvent> source = getFileStreamWithWatermark(env);

        DataStream<Integer> loginCountStream = loginCount(source);

        loginCountStream.print("login count per tumbling window: ");

        env.execute();
    }

    /**
     * non-keyed stream的窗口操作
     * 滚动窗口，10ms开一个窗，统计在这个窗口内有多少人登录（LOGIN事件）
     * @param source 已经带有timestamp和watermark的数据流
     * @return 每个窗口内的登录人数
     */
    public static DataStream<Integer> loginCount(DataStream<UserEvent> source){
        SingleOutputStreamOperator<Integer> loginCountStream = source.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10L)))

                .aggregate(new LoginCountAggregateFunction());

        return loginCountStream;
    }



    /**
     * 计算一个窗口内有多少登录事件
     */
    public static class LoginCountAggregateFunction extends RichAggregateFunction<UserEvent, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserEvent value, Integer accumulator) {
            if (value.getAction().equals(UserAction.LOGIN)){
                return accumulator+1;
            }
            return accumulator;
        }

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
