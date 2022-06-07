package com.wxwmd.connector.textfile;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author wxwmd
 * @description 将文件夹下的文件作为无界流读入
 * 程序会定期扫描文件夹，如果出现了新文件，那么就把新文件当作流再读进来
 */
public class UnBoundedFilesSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        readFiles(env);
        env.execute();
    }

    static void readFiles(StreamExecutionEnvironment env){
        String path = "flink-connectors/file-connector/src/main/resources/unbounded-files";
        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path))
                        .monitorContinuously(Duration.ofSeconds(1L))
                        .build();
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        stream.print("read unbounded files: ");
    }
}
