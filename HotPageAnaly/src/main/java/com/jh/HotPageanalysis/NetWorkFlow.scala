package com.jh.HotPageanalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
 * create by young on 2020.7.23
 * desc: 网络流量统计 热门页面
 * 每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL
 */
object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //相对路径
    val path = getClass.getResource("/apache.log")
    val dataStream = env.readTextFile(path.toString)
        .map(data=>{
          val splited: Array[String] = data.split(" ")
          //对日期进行处理17/05/2015:10:05:03
          val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val eventTime: Long = format.parse(splited(3).trim).getTime
          ApacheEventLog(splited(0).trim,splited(1).trim,eventTime,splited(5).trim,splited(6).trim)
        })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheEventLog](Time.seconds(5)) {
          override def extractTimestamp(element: ApacheEventLog): Long = {
            element.EventTime
          }
        })
        .keyBy(_.url)
        .timeWindow(Time.minutes(10),Time.seconds(5))
      /**
       * 对于此窗口而言，允许60秒的迟到数据，即第一次触发是在watermark > end-of-window时
       * 第二次（或多次）触发的条件是watermark < end-of-window + allowedLateness时间内，这个窗口有late数据到达
       */
      .allowedLateness(Time.seconds(60))
        .aggregate(new URLCount(), new WindowResult())
        .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))  //前五
      .print("process result")

    env.execute("Hot Page job")
  }
}

//构建输入数据样例类
case class ApacheEventLog(ip:String,userId:String,EventTime:Long,method:String,url:String)

// 窗口聚合结果样例类
case class UrlViewCount(url:String, windowEnd:Long,count:Long)

class URLCount() extends AggregateFunction[ApacheEventLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheEventLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow, input: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  var mapState: MapState[String, Long] =_
  override def open(parameters: Configuration): Unit = {
    mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("map-state", classOf[String], classOf[Long]))
  }

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
       mapState.put(value.url,value.count)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]

    val iter: util.Iterator[Map.Entry[String, Long]] = mapState.entries().iterator()
    while(iter.hasNext){
      val map: Map.Entry[String, Long] = iter.next()
      listBuffer += ((map.getKey,map.getValue))
    }

    val resultList: ListBuffer[(String, Long)] = listBuffer.sortWith(_._2 > _._2).take(topSize)

    val builder = new StringBuilder
    builder.append("\n").append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- resultList.indices){
      val urlViewCount = resultList(i)
      builder.append("NO.").append(i + 1)
      builder.append("  URL:").append(urlViewCount._1)
      builder.append("  访问量:").append(urlViewCount._2).append("\n")
    }
    builder.append("=========")
    out.collect(builder.toString())
    Thread.sleep(1000)
  }
}