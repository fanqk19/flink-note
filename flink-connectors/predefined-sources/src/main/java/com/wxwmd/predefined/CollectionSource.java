package com.wxwmd.predefined;

import com.wxwmd.util.model.Car;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wxwmd
 * @description 从集合中创建dataStream
 */
public class CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // readFromCollection(env);
        readFromElements(env);
        env.execute();
    }



    static void readFromCollection(StreamExecutionEnvironment env){
        List<Car> cars = new ArrayList<>();
        cars.add(new Car("捷豹" ,"XEL",30));
        cars.add(new Car("奔驰", "c260", 32));
        cars.add(new Car("奥迪", "A4L", 28));
        cars.add(new Car("捷豹","XFL", 38));

        DataStreamSource<Car> carDataStreamSource = env.fromCollection(cars);
        carDataStreamSource.print("collection source");
    }

    static void readFromElements(StreamExecutionEnvironment env){
        DataStreamSource<Car> carDataStreamSource = env.fromElements(
                new Car("捷豹", "XEL", 30),
                new Car("奔驰", "c260", 32),
                new Car("奥迪", "A4L", 28),
                new Car("捷豹", "XFL", 38)
        );

        carDataStreamSource.print("elements source");
    }
}
