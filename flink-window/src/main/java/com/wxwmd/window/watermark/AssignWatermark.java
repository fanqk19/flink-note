package com.wxwmd.window.watermark;

import com.wxwmd.util.model.UserAction;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 在进行了一些变换之后分配timestamp和watermark
 */
public class AssignWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> sourceWithWatermark = getFileStreamWithWatermark(env);
        sourceWithWatermark.print("assign watermark to stream: ");

        env.execute();
    }

    public static DataStream<UserEvent> getFileStreamWithWatermark(StreamExecutionEnvironment env){
        String filePath = "flink-window/src/main/resources/event.txt";
        DataStreamSource<String> source = env.readTextFile(filePath);

        SingleOutputStreamOperator<UserEvent> userEventStream = source.map(new UserEventMapFunction())
                .assignTimestampsAndWatermarks(new UserEventWatermarkStrategy());

        return userEventStream;
    }

    static class UserEventMapFunction implements MapFunction<String, UserEvent>{

        @Override
        public UserEvent map(String value) {
            String[] props = value.split(",");

            UserAction userAction;
            String actionStr = props[1];
            switch (actionStr){
                case "LOGIN":{
                    userAction = UserAction.LOGIN;
                    break;
                }
                case "BUY":{
                    userAction = UserAction.BUY;
                    break;
                }
                case "LOGOUT":{
                    userAction = UserAction.LOGOUT;
                    break;
                }
                default:{
                    userAction=null;
                }
            }
            UserEvent userEvent = new UserEvent(props[0], userAction, Long.parseLong(props[2]));
            return userEvent;
        }
    }


    /**
     * WatermarkStrategy 提供了一些默认的watermark生成器：
     * forBoundedOutOfOrderness：传入一个乱序时间B，当一个时间戳为T的事件到达时，将watermark设置为T-B
     * noWatermarks: 不设置watermark
     *
     * 自定义WatermarkStrategy：
     * 因为我这里从kafka中读取UserEvent时元素是不带timestamp的，
     * 因此要重写createTimestampAssigner方法为每个元素先带上时间戳
     * 之后再重写createWatermarkGenerator方法，设置watermark
     */
    static class UserEventWatermarkStrategy implements WatermarkStrategy<UserEvent> {

        @Override
        public TimestampAssigner<UserEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SourceWithWatermark.UserEventTimestampAssigner();
        }

        @Override
        public WatermarkGenerator<UserEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new SourceWithWatermark.UserEventWatermarkGeneratorOnEvent();
        }
    }

    /**
     * 从元素中提取出时间戳
     */
    static class UserEventTimestampAssigner implements TimestampAssigner<UserEvent>{

        @Override
        public long extractTimestamp(UserEvent element, long recordTimestamp) {
            return element.getTimeStamp();
        }
    }

    /**
     * watermark生成器
     * 这里展示了如何基于事件触发watermark更新
     */
    static class UserEventWatermarkGeneratorOnEvent implements WatermarkGenerator<UserEvent>{

        /**
         * 基于事件触发watermark更新
         * @param event 事件
         * @param eventTimestamp 遇见的时间戳，在上面的UserEventTimestampAssigner中被分配给事件
         * @param output watermark的输出
         */
        @Override
        public void onEvent(UserEvent event, long eventTimestamp, WatermarkOutput output) {
            Watermark watermark = new Watermark(eventTimestamp - 10L);
            output.emitWatermark(watermark);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }
}
