package com;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.MyKafkaUtil.*;

public class flinkcdc_kafka {
    public static void main(String[] args) {
//        1. 获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        2. 通过FlinkCDC构建SourceFuction
        DebeziumSourceFunction<String> SourceFuction = MySqlSource.<String>builder()
                .hostname("hadoop01")
                .port(3306)
                .username("root")
                .password("hirisun")
                .databaseList("fakebob")
//                .tableList("fakebob.test")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(SourceFuction);

//        3. 打印数据写入Kafka
        dataStreamSource.print();
        String SinkTopic = "sync_base_db";
        dataStreamSource.addSink(MyKafkaUtil.getKafkaSink(SinkTopic));

    }
}
