package com.jinghang.orderpay_detect20

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * create by young on 2020.7.31
 * desc: 订单实时对账
 */
object TxMacthDetact {
  //定义侧输出流
  val unMatchpays = new OutputTag[OrderEvent]("unMatchpays")
  val unMatchReceipt = new OutputTag[ReceiptEvent]("unMatchReceipt")

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
      .filter(_.eventType != "")
      .assignAscendingTimestamps(_.eventTime*1000L)
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

    val connStream: ConnectedStreams[OrderEvent, ReceiptEvent] = orderStream.connect(receiptStream)


    val processStream: DataStream[(OrderEvent, ReceiptEvent)] = connStream.process(new TxPayMatch())
//输出成功的
    processStream.print("order and pay success")

    //输出失败的
    processStream.getSideOutput(unMatchpays).print("order and no pay")
    processStream.getSideOutput(unMatchReceipt).print("no order and pay")
    env.execute("co job")
  }



  class TxPayMatch() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
     //定义订单状态
    var orderState: ValueState[OrderEvent] =_
    //定义支付状态
    var receiptState: ValueState[ReceiptEvent] =_

    override def open(parameters: Configuration): Unit = {
      orderState= getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order-state", classOf[OrderEvent]))
      receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("pay-state", classOf[ReceiptEvent]))
    }

    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receiptEvent: ReceiptEvent = receiptState.value()
      //为空，说明支付没来
      if(receiptEvent == null){
        orderState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
      }else{
        //如果已经有了到账事件，直接输出结果，同时清空状态
        receiptState.clear()
        out.collect((value,receiptEvent))

      }
    }

    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val orderEvent = orderState.value()
      //判断orderEvent是否为空
      if(orderEvent == null){
        //为空，说明没有订单事件，把支付到账事件 保存到状态，同时注册定时器
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
      }else{
     //不为空，有订单事件，正常输出
        orderState.clear()
        out.collect((orderEvent,value))

      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if(orderState !=null){
        //不为空，支付没来，输出到测输出流
        ctx.output(unMatchpays,orderState.value())
      }
      if(receiptState != null){
        //不为空，订单没来，输出到测输出流
        ctx.output(unMatchReceipt,receiptState.value())
      }
    }
  }
}
//支付到账样例类
case class ReceiptEvent(txId:String,payChannel:String,eventTime:Long)

