package com.wxwmd.eventtime;

import com.wxwmd.eventtime.util.UserEventDeserializationSchema;
import com.wxwmd.util.model.UserEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description
 */
public class SourceWithWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        getSourceWithWatermark(env);

        env.execute();
    }

    static void getSourceWithWatermark(StreamExecutionEnvironment env){
        String topic = "user-event";
        String consumerGroup = "";

        KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder()
                .setBootstrapServers("ubuntu:9092")
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserEventDeserializationSchema())
                .build();

        DataStreamSource<UserEvent> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaSource.print();
    }
}
