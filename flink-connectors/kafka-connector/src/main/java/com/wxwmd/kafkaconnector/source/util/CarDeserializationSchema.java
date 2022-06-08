package com.wxwmd.kafkaconnector.source.util;

import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wxwmd
 * @description 自定义反序列化器，将读入的字节数组反序列化为Car对象
 */
public class CarDeserializationSchema implements DeserializationSchema<Car> {
    @Override
    public Car deserialize(byte[] message) throws IOException {
        String str = new String(message, UTF_8);
        String[] props = str.split(",");
        Car car = new Car(props[0], props[1], Double.parseDouble(props[2]));
        return car;
    }

    @Override
    public boolean isEndOfStream(Car nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Car> getProducedType() {
        return TypeInformation.of(Car.class);
    }
}
