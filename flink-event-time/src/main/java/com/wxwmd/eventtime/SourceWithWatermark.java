package com.wxwmd.eventtime;

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



        env.execute();
    }

//    static DataStream<UserEvent> getSourceWithWatermark(StreamExecutionEnvironment env){
//        String topic = "user-event";
//        String consumerGroup = "";
//
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("ubuntu:9092")
//                .setTopics(topic)
//                .setGroupId(consumerGroup)
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//    }


}
