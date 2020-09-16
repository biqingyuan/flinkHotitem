package com.jinghang.orderpay_detect20

import java.util

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * create by young on 2020.7.27 
  * desc:  订单超时检测
  */

object OrderTimeoutWithoutCEP {

  //定义侧输出流
  val orderTimeOutputTag = new OutputTag[OrderResult]("timeout")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = getClass.getResource("/OrderLog.csv").toString
    val dataStream = env.readTextFile(path)
      .map(data=>{
        val splited = data.split(",")
        OrderEvent(splited(0).trim.toLong,splited(1).trim,splited(2).trim,splited(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime *1000L)
      .keyBy(_.useId)  //分析每个用户的订单超时


    //使用状态编程对订单数据处理
//    dataStream.process(new OrderTimeoutWarning()).print()

    val resultSteam = dataStream.process(new OrderPayMatch())
    resultSteam.print("payed")
    resultSteam.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order timeout job")
  }

  class OrderPayMatch extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{

    //定义一个是否支付的状态
    var isPayedState: ValueState[Boolean] = _
    //保存时间戳的状态
    var timeState: ValueState[Long] = _
    override def open(parameters: Configuration): Unit = {
      isPayedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state",classOf[Boolean]))
      timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state",classOf[Long]))
    }

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      //读取状态
      val isPayed = isPayedState.value()
      val timestamp = timeState.value()

      //根据事件类型判断
      if(value.eventType == "create"){
        //事件类型是create，判断是否支付过
        if (isPayed){
          //如果支付过，清空定时器，清空状态
          out.collect(OrderResult(value.useId,"payed  successfully"))
          ctx.timerService().deleteEventTimeTimer(timestamp)
          isPayedState.clear()
          timeState.clear()
        }else{
          //没有支付过，注册定时器，等待pay
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }

      }else if(value.eventType == "pay"){
        //支付事件，判断是否create，使用timer表示
        if(timestamp > 0){
          //如果定时器有值，说明有create来过
          //判断是否超时，如果状态保存的时间大于当前支付的时间 说明未超时
          if(timestamp > value.eventTime*1000L){
            //说明未超时，成功支付
            out.collect(OrderResult(value.useId,"payed  successfully"))
          }else{
            //订单超时
            ctx.output(orderTimeOutputTag,OrderResult(value.useId,"payed timeout"))
          }
          //输出结束，清空状态
          ctx.timerService().deleteEventTimeTimer(timestamp)
          isPayedState.clear()
          timeState.clear()
        }else{
          //没有create来，pay先来，注册定时器等待create
          isPayedState.update(true) //更新支付状态
          ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
          timeState.update(value.eventTime*1000L)
        }
      }
    }

    //这个onTimer，触发的全是有问题的
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //根据状态判断哪个先来
      if(isPayedState.value()){
        //如果为true 说明 pay先来，没有等到create
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"payed, but not found create"))
      }else{
        //如果为false 说明 create先来，没有等到pay,也就是未支付
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"timeout"))
      }

      isPayedState.clear()
      timeState.clear()
    }
  }



}


