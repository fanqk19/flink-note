package com.wxwmd.connector.hdfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * @author wxwmd
 * @description 从hdfs中读取文件作为data source
 */
public class HdfsSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        readHdfs(env);
        env.execute();
    }

    static void readHdfs(StreamExecutionEnvironment env){
        String filePath = "hdfs://ubuntu:9000/flink-coding/file.txt";

        DataStream<Tuple2<LongWritable, Text>> input =
                env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(), LongWritable.class, Text.class, filePath));

        input.print("read from hdfs: ");
    }
}
