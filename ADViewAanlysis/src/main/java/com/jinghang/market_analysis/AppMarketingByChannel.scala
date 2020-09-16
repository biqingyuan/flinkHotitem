package com.jinghang.market_analysis




import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
/**
 * create by young on 2020.7.24
 * desc: 推广渠道分析
 */
object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      //自定义数据源
      val dataStream = env.addSource(new SimulatedEventSource())
        .assignAscendingTimestamps(_.timestamp)
        .filter(_.behavior != "UNINSTALL") //针对非卸载用户
        .map(data => ((data.behavior, data.channel), 1L))
        .keyBy(_._1) //分析每个渠道的每种行为 推广情况
        .timeWindow(Time.hours(1), Time.seconds(10)) //每隔10s 统计近一个小时的数据
        .process(new MarketingCountByChannel())
        .print("count")

      env.execute("channel job")

    }
  }

  //数据输入样例类
  case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

  // 输出结果样例类
  case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

  //自定义数据源
  class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {

    // 定义是否运行的标识位
    var running = true
    // 定义用户行为的集合
    val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
    // 定义渠道的集合
    val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
    // 定义一个随机数发生器
    val rand: Random = new Random()

    override def cancel(): Unit = running = false

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
      // 定义一个生成数据的上限
      val maxElements = Long.MaxValue
      var count = 0L

      // 随机生成所有数据
      while (running && count < maxElements) {
        val id = UUID.randomUUID().toString
        val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
        val channel = channelSets(rand.nextInt(channelSets.size))
        val ts = System.currentTimeMillis()

        ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

        count += 1
        TimeUnit.MILLISECONDS.sleep(10L)
      }
    }
  }

  class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {

    override def process(key: (String, String),
                         context: Context, elements: Iterable[((String, String), Long)],
                         out: Collector[MarketingViewCount]): Unit = {

      val windowStart = new Timestamp(context.window.getStart).toString
      val windowEnd = new Timestamp(context.window.getEnd).toString
      val behavior = key._1
      val channel = key._2
      val count = elements.iterator.size

      out.collect(MarketingViewCount(windowStart, windowEnd, channel, behavior, count))

    }
  }


