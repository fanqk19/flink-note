package com.wxwmd.predefined;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wxwmd
 * @description 从socket读取消息作为数据源
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        readSocket(env);
        env.execute();
    }

    static void readSocket(StreamExecutionEnvironment env){
        /*
        我用的主机地址是ubuntu，端口是12345
        linux机器开端口： nc -lk 12345
        记得防火墙打开这个端口
         */
        DataStreamSource<String> socketTextStream = env.socketTextStream("ubuntu", 12345);
        socketTextStream.print("socket data source: ");
    }
}
