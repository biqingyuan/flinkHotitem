package com.jh.HotPageanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //相对路径
    val path = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(path.toString) .map(data =>{
      val splited = data.split(",")
      UserBehavior(splited(0).trim.toLong,splited(1).trim.toLong,splited(2).trim.toLong,splited(3).trim,splited(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000L)

    dataStream.filter(_.behavior == "pv")
      .map(data => ("pv",1))
      .keyBy(0)
      .timeWindow(Time.hours(1))
      .sum(1)
      .print("pv count")

    env.execute("pv job")
  }
}

//输入数据样例类
case class UserBehavior(userId:Long,itemsId:Long,categoryId:Long,behavior:String,timestamp:Long)