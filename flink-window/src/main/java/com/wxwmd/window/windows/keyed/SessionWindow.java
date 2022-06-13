package com.wxwmd.window.windows.keyed;

import com.wxwmd.util.model.UserEvent;
import com.wxwmd.window.util.CountFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.wxwmd.window.watermark.AssignWatermark.getSocketStreamWithWatermark;

/**
 * @author wxwmd
 * @description 会话窗口
 * 此代码的说明文档写在https://blog.csdn.net/cobracanary/article/details/125260627
 *
 * 使用方法：
 * 打开端口
 * 在端口中逐次输入：
 * bob,LOGIN,1654688058
 * burg,LOGIN,1654688058
 * bob,BUY,1654688064
 * doris,LOGIN,1654688081
 * burg,BUY,1654688084
 * doris,LOGOUT,1654688089
 * wxwmd,LOGIN,1654688100
 * duck,LOGIN,1654688096
 * note：这是窗口聚合计算，所以不是输入一条就输出一条的
 */
public class SessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        //1. 使用socket作为data source
        DataStream<UserEvent> source = getSocketStreamWithWatermark(env, "localhost", 12345);
        //2.  使用本地文件作为data source
        //DataStream<UserEvent> source = getFileStreamWithWatermark(env);

        DataStream<String> countStream = countEventBySessionWindow(source);
        countStream.print("event count");

        env.execute();
    }

    static DataStream<String> countEventBySessionWindow(DataStream<UserEvent> source){
        SingleOutputStreamOperator<String> countStream = source.keyBy(UserEvent::getUserAction)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(5)))
                .process(new CountFunction());

        return countStream;
    }
}
