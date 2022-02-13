package com;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumChangeConsumer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flinkcdc {
    public static void main(String[] args) throws Exception {
//        1. 获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        2. 通过FlinkCDC构建SourceFuction
        DebeziumSourceFunction<String> SourceFuction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("test")
                .databaseList()
                .tableList()
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(SourceFuction);


//        打印数据
        dataStreamSource.print();

//    启动任务
        env.execute("flinkCDC");
    }
}
