##### flink为kafka提供的connector
> kafka以其高吞吐、高可用的特性称为数据开发中最流行的消息队列之一，flink官方提供了kafka的connector，其文档在[kafka connector](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/kafka/)。
> kafka connector提供了读取kafka和写入kafka的功能，详见文档中的`KafkaSource`和`KafkaSink`。
> `KafkaSource`功能包括订阅kafka topic、指定topic的分区partition、偏移量offest；指定kafka消息的反序列化器。
>`KafkaSink`功能包括将消息写入kafka topic、指定序列化器、指定sink是否精确一次。 