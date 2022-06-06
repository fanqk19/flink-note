package com.wxwmd.predefined;

import com.wxwmd.util.model.Car;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.net.URL;

/**
 * @author wxwmd
 * @description 从文件中创建DataSource
 * 为了演示，直接用resources下的本地文件做了示例，生产环境中可以替换为hdfs上的文件
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // readFromTextFile(env);
        readPojoFromCsv(env);
        env.execute();
    }

    // 直接调用readTextFile，将文本中的每一行当作流中的一个元素
    static void readFromTextFile(StreamExecutionEnvironment env){
        String filePath = FileSource.class.getResource("/file1.txt").getPath();
        DataStreamSource<String> textFileSource = env.readTextFile(filePath);
        textFileSource.print("text file source: ");
    }

    // 从csv文件读取对象，等效于readTextFile + map(string->POJO)
    static void readPojoFromCsv(StreamExecutionEnvironment env){
        String filePath = FileSource.class.getResource("/cars.csv").getPath();

        PojoTypeInfo<Car> carPojoTypeInfo = (PojoTypeInfo<Car>) TypeInformation.of(Car.class);
        PojoCsvInputFormat<Car> inputFormat = new PojoCsvInputFormat<>(new Path(filePath), carPojoTypeInfo);

        DataStreamSource<Car> carStreamSource = env.createInput(inputFormat, carPojoTypeInfo);
        carStreamSource.print("read POJO from CSV: ");
    }
}
