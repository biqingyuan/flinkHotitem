package com.jinghang.market_analysis

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * create by young on 2020.7.24
 * desc: 计算广告点击量 黑名单机制
 */
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )
object AdStatisticsByGeoWithBlackList {
  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

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

    val filterStream = dataStream
      .keyBy(data => (data.userId,data.adId))  //每个用户对某个广告的点击量分析  如果超过规定次数则认为是恶意点击，加入黑名单
      .process(new FilterBlackListUser(100))  //定义最大点击次数

        filterStream.keyBy(_.province)
        .timeWindow(Time.hours(1),Time.seconds(5))
            .aggregate( new AdCountAgg(), new AdCountResult())
            .print("result")

    filterStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("AdClick job")
  }
  //内部类
  class FilterBlackListUser(maxCount:Long) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{

    //保存点击次数的状态
    var countState: ValueState[Long] = _
    // 保存是否发送过黑名单的状态
    var isSentBlackList: ValueState[Boolean] = _
    // 保存定时器触发的时间戳
    var resetTimer: ValueState[Long] = _
    override def open(parameters: Configuration): Unit = {

      countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
      isSentBlackList = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
      resetTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-Timer", classOf[Long]))
    }

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(Long, Long),
                                  AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {
      //拿到当前的点击次数
      val currCount = countState.value()

      //第一点击， 注册定时器每天 00：00 触发
      if(currCount == 0){
        //计算明天0点时间
        val ts = ctx.timerService().currentProcessingTime()/(1000*60*60*24 + 1)*24*3600*1000L
        resetTimer.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
      }else{
        //说已经有点击次数  和规定的点击次数做比较
        // 判断计数是否达到上限，如果到达则加入黑名单
        if(currCount >= maxCount){
          //判断是否已经加入过黑名单
          if(!isSentBlackList.value()){
            isSentBlackList.update(true)
            //同时结果输出到侧输出流
            ctx.output(blackListOutputTag,BlackListWarning(value.userId,value.adId,"点击次数已经超过 "+maxCount+" 次"))
          }
          //已经加入过  直接返回
          return
        }


      }
      //更点击次数  当前点击次数+1
      countState.update(currCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long),
                           AdClickEvent, AdClickEvent]#OnTimerContext,
                         out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if(timestamp == resetTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}



