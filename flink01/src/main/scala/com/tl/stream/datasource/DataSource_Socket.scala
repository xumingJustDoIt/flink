package com.tl.stream.datasource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSource_Socket {

  def main(args: Array[String]): Unit = {

    //1. 创建流式环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 构建socket数据源
//    val socketDataStream: DataStream[String] = env.socketTextStream("node01",9999)
//    val socketDataStream: DataStream[String] = env.socketTextStream("10.123.121.153",9999)

    import org.apache.flink.api.java.utils.ParameterTool
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val socketDataStream: DataStream[String] = env.socketTextStream(host,port)

    // 3. 数据转换,空格切分
    val mapDataStream: DataStream[String] = socketDataStream.flatMap(
      line => line.split(" ")
    )

    // 4. 打印
    mapDataStream.print()

    // 5. 执行任务
    env.execute()

  }

}
