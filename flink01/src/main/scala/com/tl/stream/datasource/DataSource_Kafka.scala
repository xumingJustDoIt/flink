package com.tl.stream.datasource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.flink.api.scala._

object DataSource_Kafka {

  def main(args: Array[String]): Unit = {

    // 1. 创建流式环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2 .指定kafka相关信息
    val kafkaCluster = "10.123.121.185:21007"  // node01:9092,node02:9092,node03:9092
    val kafkaTopic = "topic01"
    // 3. 创建Kafka数据流
    val props = new Properties()
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,kafkaCluster)

    props.setProperty("group.id", "test01")
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT")
    props.setProperty("sasl.mechanism","GSSAPI")
    props.setProperty("sasl.kerberos.service.name","kafka")

    val flinkKafkaConsumer = new FlinkKafkaConsumer010[String](kafkaTopic,new SimpleStringSchema(),props)

    //4 .设置数据源
    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    // 5. 打印数据
    kafkaDataStream.print()


    // 6.执行任务
    env.execute()

  }

}
