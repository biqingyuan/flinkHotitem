package com.jinghang.orderpay_detect20

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMacthByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取订单事件流
    val path1 = getClass.getResource("/OrderLog.csv").toString
    val orderStream = env.readTextFile(path1)
      .map(data=>{
        val splited = data.split(",")
        OrderEvent(splited(0).trim.toLong,splited(1).trim,splited(2).trim,splited(3).trim.toLong)
      })
      .filter(_.txID != "")
      .assignAscendingTimestamps(_.eventTime *1000L)
      .keyBy(_.txID)

    //读取支付到账事件流
    val path2 = getClass.getResource("/ReceiptLog.csv").toString
    val receiptStream = env.readTextFile(path2)
      .map(data=>{
        val splited = data.split(",")
        ReceiptEvent(splited(0).trim,splited(1).trim,splited(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime *1000L)
      .keyBy(_.txId)

    //合并订单流和支付到账流，共同处理
    orderStream.intervalJoin(receiptStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TxMatchByIntervalJoin())
      .print("in 10s Match success")

    env.execute("Match join job")
  }
}
class TxMatchByIntervalJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent,
                              right: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}