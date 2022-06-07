package com.wxwmd.kafkaconnector.source;

import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
public class KafkaDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Car> cars = readFromKafka(env, "cars", "");
        cars.print("kafka data source: ");

        env.execute();
    }

    static DataStream<Car> readFromKafka(StreamExecutionEnvironment env, String topic, String consumerGroup){
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("ubuntu:9092")
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<Car> carDataStream = kafkaSource.map((MapFunction<String, Car>) str->{
            String[] props = str.split(",");
            Car car = new Car(props[0], props[1], Double.parseDouble(props[2]));
            return car;
        });

        return carDataStream;
    }
}
