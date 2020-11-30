package com.tl.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object BatchFromFile {

  def main(args: Array[String]): Unit = {

    // 创建env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 加载文件
    val textDataSet: DataSet[String] = env.readTextFile("data\\data.txt")

    // 打印
    textDataSet.print()

  }
}
