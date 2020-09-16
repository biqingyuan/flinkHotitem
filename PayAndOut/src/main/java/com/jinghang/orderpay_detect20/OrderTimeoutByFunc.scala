//package com.jinghang.orderpay_detect20
//
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.util.Collector
//
//object OrderTimeoutByFunc {
//  //定义侧输出流
//  val orderTimeOutputTag = new OutputTag[OrderResult]("timeout")
//
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val path = getClass.getResource("/OrderLog.csv").toString
//    val dataStream = env.readTextFile(path)
//      .map(data=>{
//        val splited = data.split(",")
//        OrderEvent(splited(0).trim.toLong,splited(1).trim,splited(2).trim,splited(3).trim.toLong)
//      })
//      .assignAscendingTimestamps(_.eventTime *1000L)
//      .keyBy(_.useId)  //分析每个用户的订单超时
//
//    val resultStream = dataStream.process(new OrderPayMatch())
//    resultStream.print("success")
//    resultStream.getSideOutput(orderTimeOutputTag).print("fail")
//    env.execute("order job")
//  }
//  class OrderPayMatch() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
//    //定义一个是否支付的状态，初始值为false
//    var isPayedState: ValueState[Boolean] = _
//    //保存时间戳的状态 初始值为0L
//    var timeState: ValueState[Long] = _
//
//    override def open(parameters: Configuration): Unit = {
//      isPayedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isOutof-state", classOf[Boolean]))
//      timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", classOf[Long]))
//    }
//
//    override def processElement(value: OrderEvent,
//                                ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
//                                out: Collector[OrderResult]): Unit = {
//      //取出状态的值
//      val isPayed: Boolean = isPayedState.value()
//      val timeStamp: Long = timeState.value()
//      //先根据事件进行判断
//      if(isPayed == "create"){
//        //事件类型是create，判断是否支付过
//        if(isPayed){
//          //如果支付过，清空定时器，清空状态
//          out.collect(OrderResult(value.useId,"payed successfully"))
//          ctx.timerService().deleteEventTimeTimer(timeStamp)
//          isPayedState.clear()
//          timeState.clear()
//        }else {
//          //没有支付过，注册定时器，等待pay
//          //定时器为15分钟之后
//          val ts = value.eventTime*1000L + 15 * 60 * 1000L
//          ctx.timerService().registerEventTimeTimer(ts)
//          timeState.update(ts)
//        }
//
//
//      }else if(isPayed == "pay"){
//        //支付事件，判断是否create，使用timer表示
//        if(timeStamp > 0){
//          //如果定时器有值，说明有create来过
//          //判断是否超时，如果状态保存的时间大于当前支付的时间 说明未超时
//          if(timeStamp > value.eventTime*1000L){
//            //说明未超时，成功支付
//            out.collect(OrderResult(value.useId,"payed  successfully"))
//          }else{
//            //订单超时
//            ctx.output(orderTimeOutputTag,OrderResult(value.useId,"payed timeout"))
//          }
//          //输出结束，清空状态
//          ctx.timerService().deleteEventTimeTimer(timeStamp)
//          isPayedState.clear()
//          timeState.clear()
//        }else{
//          //没有create来，pay先来，注册定时器等待create
//          isPayedState.update(true)
//          ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
//          timeState.update(value.eventTime*1000L)
//        }
//      }
//
//    }
//    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
//      //根据状态判断哪个先来
//      if(isPayedState.value()){
//        //如果为true 说明 pay先来，没有等到create
//        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"payed, but not found create"))
//      }else{
//        //如果为false 说明 create先来，没有等到pay
//        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"timeout"))
//      }
//
//      isPayedState.clear()
//      timeState.clear()
//    }
//  }
//}
//
//
