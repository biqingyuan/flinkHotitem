package com.jinghang.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = getClass.getResource("/LoginLog.csv").toString

    val dataStream = env.readTextFile(path)
      //    val dataStream =env.socketTextStream("hadoop01",7777)
      .map(data=>{
        val splited = data.split(",")
        LoginEvent(splited(0).trim.toLong,splited(1).trim,splited(2).trim,splited(3).trim.toLong)
      })
      //wartermark  有界无序
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(2)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime*1000L
      })
      .keyBy(_.userId)  //按照用户

    //1.定义一个模式匹配规则   2s内连续登陆失败
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("start")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //2.将规则应用到数据流中
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream, pattern)

    //3.从pattern stream上应用select function，检出匹配事件序列
    patternStream.select(new LoginFailMatch())
        .print("fail")
    env.execute("login fail with cep job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail: LoginEvent = map.get("start").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail")
  }
}
