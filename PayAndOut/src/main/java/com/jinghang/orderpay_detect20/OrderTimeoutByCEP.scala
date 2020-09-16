package com.jinghang.orderpay_detect20

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeoutByCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = getClass.getResource("/OrderLog.csv").toString
    val datastream = env.readTextFile(path)
      .map(data => {
        val splited = data.split(",")
        OrderEvent(splited(0).trim.toLong, splited(1).trim, splited(2).trim, splited(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.useId)

    //1.定义一个规则
    val pattern = Pattern
      .begin[OrderEvent]("start")
      .where(_.eventType == "create")
      .followedBy("follow")//使用的是非严格近邻
      .where(_.eventType == "pay")
      .within(Time.minutes(15))  //规定时间 15分钟内
    //2.将规则应用到数据流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(datastream, pattern)

    //3.从select方法 提取事件，超时时间做出一个报警
    val outOut: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeOut")

    val resultStream: DataStream[OrderResult] = patternStream.select(outOut, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("pay")
    resultStream.getSideOutput(outOut).print("fail")
    env.execute("Order")
  }
}
//输入数据的样例类
case class OrderEvent(useId:Long,eventType:String,txID:String,eventTime:Long)
//输入结果的样例类
case class OrderResult(userId:Long,result:String)

//订单超时的时间处理
class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]],
                       timeoutTimestamp: Long): OrderResult = {
    val userId: Long = pattern.get("start").iterator().next().useId

    OrderResult(userId,"OutofTime!!!!!!!!")
  }
}

//正常支付事件
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val userId: Long = pattern.get("follow").iterator().next().useId

    OrderResult(userId,"success")
  }
}