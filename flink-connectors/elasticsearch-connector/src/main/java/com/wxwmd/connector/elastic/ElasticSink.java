package com.wxwmd.connector.elastic;

import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wxwmd
 * @description 将flink datastream中的元素sink到elastic search中
 */
public class ElasticSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        sinkToElastic(env, "cars");

        env.execute();
    }

    static void sinkToElastic(StreamExecutionEnvironment env, String index){
        SingleOutputStreamOperator<Car> source = env.readTextFile("flink-connectors/elasticsearch-connector/src/main/resources/cars.csv")
                .map((MapFunction<String, Car>) str -> {
                    String[] props = str.split(",");
                    return new Car(props[0], props[1], Double.parseDouble(props[2]));
                });

        source.sinkTo(
                new Elasticsearch7SinkBuilder<Car>()
                        // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
                        .setBulkFlushMaxActions(1)
                        .setHosts(new HttpHost("ubuntu", 9200, "http"))
                        .setEmitter(
                                (element, context, indexer) ->
                                        indexer.add(createIndexRequest(index, element)))
                        .build());
    }

    static IndexRequest createIndexRequest(String index, Car car) {
        // 构造要sink的内容，其实有点类似json
        Map<String, Object> carInfo = new HashMap<>();
        carInfo.put("Brand", car.getBrand());
        carInfo.put("Model", car.getModel());
        carInfo.put("price", car.getPrice());

        return Requests.indexRequest()
                .index(index)
                .id(String.format("%s-%s", car.getBrand(), car.getModel()))
                .source(carInfo);
    }
}
