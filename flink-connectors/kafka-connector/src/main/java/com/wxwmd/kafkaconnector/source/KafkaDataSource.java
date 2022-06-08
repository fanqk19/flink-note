package com.wxwmd.kafkaconnector.source;

import com.wxwmd.kafkaconnector.source.util.CarDeserializationSchema;
import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 从美国kafka topic中读取元素作为data source
 */
public class KafkaDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Car> cars = readFromKafka(env, "cars", "");
        cars.print("kafka data source: ");

        env.execute();
    }

    static DataStream<Car> readFromKafka(StreamExecutionEnvironment env, String topic, String consumerGroup){
        KafkaSource<Car> source = KafkaSource.<Car>builder()
                .setBootstrapServers("ubuntu:9092")
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CarDeserializationSchema())
                .build();

        DataStreamSource<Car> carDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        return carDataStream;
    }
}
