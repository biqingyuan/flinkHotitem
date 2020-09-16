package com.jh.flinkHotItemanalyis

import java.sql.Timestamp
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItemAnalyis {
  def main(args: Array[String]): Unit = {
   //设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(2)
   //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置水印的间隔
    env.getConfig.setAutoWatermarkInterval(500L)
    //加载数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bqy01:9092,bqy02:9092,bqy03:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val ds = env.addSource(new FlinkKafkaConsumer[String]("test02",new SimpleStringSchema(),properties))
    //对数据进行处理，将一行行的数据放入自定义类UserBehavior
    ds.map(data=>{
      val splited: Array[String] = data.split(",")
      UserBehavior(splited(0).trim.toLong,splited(1).trim.toLong,splited(2).trim.toLong,splited(3).trim,splited(4).trim.toLong)
    })
      //自动提交watermark，但只适用于已经排好顺序的数据
      //.assignAscendingTimestamps(_.timestamp*1000L)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(5)) {
        override def extractTimestamp(element: UserBehavior): Long = {
          element.timestamp*1000L
        }
      })
      //只留下pv的数据
      .filter(_.behavior == "pv")
      //先按itemsId进行分区
      .keyBy(_.itemsId)
      //滑动窗口
      .timeWindow(Time.minutes(2),Time.minutes(1))
      //对数据进行聚合，new AggCount() 对数据进行聚合的规则（预聚合），new WindowResult() 输出结果的类型
      .aggregate(new AggCount(), new WindowResult())
      //再按照windowEnd分区
      .keyBy(_.windowEnd)
      //对数据进行处理，只取排名前三的数据
      .process(new TopNHotItems(3))//3为排名前三的商品

      .print("process result")

    env.execute("hotItems job")
  }
}
//用户行为的样例类
case class UserBehavior(userId:Long ,itemsId:Long ,categoryId:Long ,behavior:String,timestamp:Long)

//定义输出结果的样例类
case class ItemViewCount(itemsId:Long, windowEnd:Long, count:Long)

//定义的预聚合的规则
class AggCount() extends AggregateFunction[UserBehavior,Long,Long]{
  //创建累加器 赋初始值是0
  override def createAccumulator(): Long = 0L
  //来一条数据 +1 进行累加
  override def add(value: UserBehavior, acc: Long): Long = acc + 1
  //从累加器里面取值@return The final aggregation result.
  override def getResult(acc: Long): Long = acc
  //合并计算的结果
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

//输出结果的类型
//第一个long是累加器的类型，第二个long是key的类型
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long,
                     window: TimeWindow, input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {
    //key ： 商品id
    //window.getEnd ：获取窗口的截止时间
    //input.iterator.next() 取出聚合的结果
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

//定义列表状态 保存每个itemid的状态
class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  //定义列表状态 保存每个itemid的状态
//  ListState 是 Flink 提供的类 似 Java List 接口 的 State API，它集成了框架的 checkpoint 机 制，
//  自动 做到了 exactly-once 的语义保证
  private var listState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("list-state",classOf[ItemViewCount]))
  }
  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    //每来一条数据 保存状态
    listState.add(value)
    //同时注册定时器 在onTimer进行触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //创建scala lits接收数据
    val list = new ListBuffer[ItemViewCount]

    //从列表状态取值  添加到list 集合
    val iter: util.Iterator[ItemViewCount] = listState.get().iterator()
    while(iter.hasNext){
      list += iter.next()
    }
    listState.clear()
    //对集合里面的结果排序  降序
    val resultList: ListBuffer[ItemViewCount] = list.sortBy(_.count).reverse.take(topSize)

    //结果拼接字符串输出
    val builder: StringBuilder = new StringBuilder
    //因为之前的触发器，已经加上1，所以这里减去1
    builder.append("时间：").append(new Timestamp(timestamp-1)).append("\n")

    for(i <- resultList.indices){
      builder.append("No.").append(i + 1).append(":")
      val itemsId = resultList(i).itemsId
      val count = resultList(i).count
      builder.append("  商品Id: ").append(itemsId)
      builder.append("  浏览量: ").append(count)
        .append("\n")
    }
    builder.append("======================").append("\n")

    // 控制输出频率
    Thread.sleep(1000)
    out.collect(builder.toString())
  }
}