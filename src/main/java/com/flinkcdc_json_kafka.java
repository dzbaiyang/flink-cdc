package com;

import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

public class flinkcdc_json_kafka {
    public static void main(String[] args) throws Exception {
//1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建Flink-MySQL-CDC的Source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop01")
                .port(3306)
                .username("root")
                .password("hirisun")
                .databaseList("fakebob")
//                .tableList("practice.user")
                .serverTimeZone("Asia/Shanghai")
//                    .startupOptions(StartupOptions.latest())
                .startupOptions(StartupOptions.earliest())  //earlist是从binlog第一行开始  但是earlist有个限制就是你必须在建库之前就开启binlog  如果你是中途开启binlog earlist会有问题
//                    .startupOptions(StartupOptions.initial())
//                .startupOptions(KafkaOptions.StartupOptions.class)
                .deserializer(new StringDebeziumDeserializationSchema())//官方告诉需要反序列化
                .deserializer(new CustomerDeserialization())
                .build();

        //3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
//            DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
        mysqlDS.print();
        //4.打印数据

        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("sync_base_db"));

        //5.执行任务
        env.execute("Flinck-CDC-Kafka");
    }
}