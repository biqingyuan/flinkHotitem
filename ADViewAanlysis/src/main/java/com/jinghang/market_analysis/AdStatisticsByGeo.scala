package com.jinghang.market_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * create by young on 2020.7.24
 * desc: 计算广告点击量
 */
object AdStatisticsByGeo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = getClass.getResource("/AdClickLog.csv")
    val dataStream = env.readTextFile(path.toString)
    .map(data=>{
      val splited = data.split(",")
        AdClickEvent(splited(0).trim.toLong,splited(1).trim.toLong,splited(2).trim,splited(3).trim,splited(4).trim.toLong)
    })
        .assignAscendingTimestamps(_.timestamp*1000L)
        .keyBy(_.province)
        .timeWindow(Time.hours(1),Time.seconds(5))
        .aggregate(new AdCountAgg(), new AdCountResult())
      .print("result")

    env.execute("AdStatisticsByGeo")
  }
}

//输入数据的样例类
case class AdClickEvent(userId:Long, adId:Long,province: String, city: String, timestamp: Long)
case class AdClickEvent1(userId:Long,province: String, city: String, timestamp: Long)

//定义输出结果的样例类
case class AdViewCount(province: String,windowEnd:Long,count:Long)

class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long,AdViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdViewCount]): Unit = {
    out.collect(AdViewCount(key,window.getEnd,input.iterator.next()))
  }
}