package com;//package com;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import java.util.Properties;
//public class MyKafkaUtil {
//    private static String KAFKA_SERVER = "hadoop01:9092,hadoop02:9092,hadoop03:9092";
//    private static Properties properties =  new Properties();
//    static {
//        properties.setProperty("bootstrap.servers",KAFKA_SERVER);
//    }
//    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
//        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
//    }
//}

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String KAFKA_SERVER = "hadoop01:9092,hadoop02:9092,hadoop03:9092";//Kafka的集群和端口
    private static Properties properties =  new Properties();
    static {
        properties.setProperty("bootstrap.servers",KAFKA_SERVER);
    }
    //返回一个kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }
    // //返回一个kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }
}