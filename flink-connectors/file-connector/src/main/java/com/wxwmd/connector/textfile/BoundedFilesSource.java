package com.wxwmd.connector.textfile;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 将一个文件夹下的文件作为有界流读入
 *  程序只会读取这个文件夹一次，然后关闭读入
 */
public class BoundedFilesSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        readFiles(env);
        env.execute();
    }

    static void readFiles(StreamExecutionEnvironment env){
        // 得到文件夹路径
        String path = BoundedFilesSource.class.getResource("flink-connectors/file-connector/src/main/resources/bounded-files").getPath();

        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path))
                        .build();
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        stream.print("read bounded files: ");
    }
}
