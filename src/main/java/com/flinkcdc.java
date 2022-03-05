package com;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;

public class flinkcdc {
    public static void main(String[] args) throws Exception {
//        1. 获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        1.1 开启CheckPoint
//
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.setStateBackend(new FsStateBackend  ("hdfs://hadoop01:8020/flink/flink-checkpoints"));

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


//        打印数据
        dataStreamSource.print();

//    启动任务
        env.execute("flinkCDC");
    }
}
