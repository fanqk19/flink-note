package com.wxwmd.kafkaconnector.sink;

import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 将resources/cars.csv下的数据读出，并写道kafka topic中。
 */
public class KafkaDataSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        sinkToKafka(env,"cars");

        env.execute();
    }

    static void sinkToKafka(StreamExecutionEnvironment env, String topic){
        // 从文件中读取数据流
        String path = "flink-connectors/kafka-connector/src/main/resources/cars.csv";
        SingleOutputStreamOperator<Car> source = env.readTextFile(path)
                .map((MapFunction<String, Car>) str -> {
                    String[] props = str.split(",");
                    Car car = new Car(props[0], props[1], Double.parseDouble(props[2]));
                    return car;
                });

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("ubuntu:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 设置sink语义
                .build();

        source.map(car -> car.toString())
                .sinkTo(kafkaSink);
    }
}
