package com.jinghang.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * create by young on 2020.7.24
 * desc: 推广渠道分析 统计总的推广量
 */
object AppMarketing {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //自定义数据源
    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data=>("behavior",1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new CountAgg(),new MarketingCountTotal())
      .print("result")

    env.execute("total job")
  }
}
class CountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + value._2

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketingCountTotal() extends WindowFunction[Long,MarketingViewCount,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow, input: Iterable[Long],
                     out: Collector[MarketingViewCount]): Unit = {
//     out.collect(MarketingViewCount(window.getStart,window.getEnd,key,input.iterator.next())
    val start = new Timestamp(window.getStart).toString
    val end = new Timestamp(window.getEnd).toString

    out.collect(MarketingViewCount(start,end,"all","all",input.iterator.next()))
  }
}