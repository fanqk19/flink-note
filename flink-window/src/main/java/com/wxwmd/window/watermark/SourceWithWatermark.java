package com.wxwmd.window.watermark;


import com.wxwmd.util.model.UserEvent;
import com.wxwmd.window.util.UserEventDeserializationSchema;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 在数据源读入时就带有timestamp和watermark
 */
public class SourceWithWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = getSourceWithWatermark(env);
        source.print("source with timestamp and watermark: ");

        env.execute();
    }

    static DataStream<UserEvent> getSourceWithWatermark(StreamExecutionEnvironment env){
        String topic = "user-event";
        String consumerGroup = "";

        KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder()
                .setBootstrapServers("ubuntu:9092")
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserEventDeserializationSchema())
                .build();

        DataStreamSource<UserEvent> kafkaSource = env.fromSource(source, new UserEventWatermarkStrategy(), "Kafka Source");
        return kafkaSource;
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
            return new UserEventTimestampAssigner();
        }

        @Override
        public WatermarkGenerator<UserEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new UserEventWatermarkGeneratorOnEvent();
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
