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

/**
 * @author wxwmd
 * @description 从文件中创建DataSource
 * 为了演示，直接用resources下的本地文件做了示例，生产环境中可以替换为hdfs上的文件
 * tips: 如果想读取整个文件夹，请参考`file-connector`模块。
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //readFromTextFile(env);
        //readPojoFromCsv(env);
        readFileContinuously(env);
        env.execute();
    }


    /**
     * 直接调用readTextFile，将文本中的每一行当作流中的一个元素
     * @param env StreamExecutionEnvironment
     */
    static void readFromTextFile(StreamExecutionEnvironment env){
        String filePath = "flink-connectors/predefined-sources/src/main/resources/file1.txt";
        DataStreamSource<String> textFileSource = env.readTextFile(filePath,"utf-8");
        textFileSource.print("text file source: ");
    }


    /**
     * 从csv文件读取对象，等效于readTextFile + map(string->POJO)
     * @param env StreamExecutionEnvironment
     */
    static void readPojoFromCsv(StreamExecutionEnvironment env){
        String filePath = "flink-connectors/predefined-sources/src/main/resources/cars.csv";

        PojoTypeInfo<Car> carPojoTypeInfo = (PojoTypeInfo<Car>) TypeInformation.of(Car.class);
        PojoCsvInputFormat<Car> inputFormat = new PojoCsvInputFormat<>(new Path(filePath), carPojoTypeInfo);

        DataStreamSource<Car> carStreamSource = env.createInput(inputFormat, carPojoTypeInfo);
        carStreamSource.print("read POJO from CSV: ");
    }

    /**
     * 持续地监控文件
     * 注意：并不是增量地处理新添加的行
     * 当文件内容发生变化时，将整个文件当作流再处理一次
     * @param env StreamExecutionEnvironment
     */
    static void readFileContinuously(StreamExecutionEnvironment env){
        String filePath = "flink-connectors/predefined-sources/src/main/resources/continuously.txt";

        DataStreamSource<String> continuousTextSource =
                env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000L);

        continuousTextSource.print("read file continuously: ");
    }

}
