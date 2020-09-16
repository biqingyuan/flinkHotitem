package com.jh.HotPageanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * create by young on 2020.7.23
 * desc: 页面独立访客数uv 统计
 */
object UniqueView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //相对路径
    val path = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(path.toString)
      .map(data =>{
        val splited = data.split(",")
        UserBehavior(splited(0).trim.toLong,splited(1).trim.toLong,splited(2).trim.toLong,splited(3).trim,splited(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
    dataStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //设置窗口
      .apply(new UvCountByWindow())
      .print("uv count")
    env.execute("uv job")
  }
}
//输出结果的样例类
case class UvViewCount(windowEnd:Long,count:Long)

//去重求uv
class UvCountByWindow extends AllWindowFunction[UserBehavior,UvViewCount,TimeWindow]{
  override def apply(window: TimeWindow,
                     input: Iterable[UserBehavior], out: Collector[UvViewCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var set: Set[Long] = Set[Long]()
    for(a <- input){
     set += a.userId
    }
    out.collect(UvViewCount(window.getEnd,set.size))

  }
}