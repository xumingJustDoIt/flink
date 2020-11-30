package com.tl.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

object BatchFromFolder {



  def main(args: Array[String]): Unit = {

    // env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 读取目录
    def params: Configuration = new Configuration()
    params.setBoolean("recursive.file.enumeration",true)
//    val folderDataSet: DataSet[String] = env.readTextFile("F:\\code\\bigdata\\flink01\\src\\main\\resources\\").withParameters(params)
    val folderDataSet: DataSet[String] = env.readTextFile("data\\").withParameters(params)

    folderDataSet.print()

  }
}
