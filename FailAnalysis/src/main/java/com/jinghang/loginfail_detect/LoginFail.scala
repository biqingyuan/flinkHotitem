//package com.jinghang.loginfail_detect
//
//import java.{lang, util}
//
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
//import scala.collection.mutable.ListBuffer
///**
// * create by young on 2020.7.25
// * desc: 恶意登陆失败检测
// * 连续登陆失败两次，输出报警信息
// */
//object LoginFail {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val path = getClass.getResource("/LoginLog.csv").toString
//    val datastream: DataStream[LoginEvent] = env.readTextFile(path)
//      .map(data => {
//        val splited = data.split(",")
//        LoginEvent(splited(0).trim.toLong, splited(1).trim, splited(2).trim, splited(3).trim.toLong)
//      })
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(2)) {
//        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
//      })
//    datastream.keyBy(_.userId)
////        .process(new LoginWarning(2)) //根据次数判断)
//      .process(new LoginWarning2(2)) //根据时间判断  2s内连续登陆失败
//        .print("warning")
//    env.execute("job")
//  }
//}
//
//
//// 输入的登录事件样例类
//case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
//// 输出的异常报警信息样例类
//case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)
////对次数进行判断
//class LoginWarning(maxFailCount:Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
//  //每条数据保存的状态
//  var loginFailState: ListState[LoginEvent] =_
//
//  override def open(parameters: Configuration): Unit = {
//    loginFailState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))
//  }
//
//  override def processElement(value: LoginEvent,
//                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
//                              out: Collector[Warning]): Unit = {
//    //从状态里面取值
//    val loginEventIter: lang.Iterable[LoginEvent] = loginFailState.get()
//    //value  判断来的这一条数据的状态  如果是失败
//    if(value.eventType == "fail"){
//      //判断是否是第一次失败  如果是第一次失败
//      if(!loginEventIter.iterator().hasNext){
//       //注册定时器,延迟 2s
//        ctx.timerService().registerEventTimeTimer(value.eventTime*1000L+2000L)
//      }
//      loginFailState.add(value)
//    }else{
//      //如果登陆成功  清空状态
//      loginFailState.clear()
//    }
//
//  }
//
//  override def onTimer(timestamp: Long,
//                       ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
//                       out: Collector[Warning]): Unit = {
//    //创建一个集合保存状态
//    val list: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
//    //取出状态
//    val loginFailEvents: util.Iterator[LoginEvent] = loginFailState.get().iterator()
//    while (loginFailEvents.hasNext){
//      list += loginFailEvents.next()
//    }
//    //比较登陆失败的次数 和 最大失败次数
//    if(list.size >= maxFailCount){
//      out.collect(Warning(list.head.userId,list.head.eventTime,list.last.eventTime,"warning login fail over "+maxFailCount+" times"))
//    }
//
//
//  }
//}
////对时间进行判断
//class LoginWarning2(maxFailCount:Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
//
//  //每条数据保存的状态
//  var loginFailState: ListState[LoginEvent] = _
//  override def open(parameters: Configuration): Unit = {
//    loginFailState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))
//  }
//
//
//  override def processElement(value: LoginEvent,
//                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
//                              out: Collector[Warning]): Unit = {
//    val loginEventIter: lang.Iterable[LoginEvent] = loginFailState.get()
//
//    //登陆失败
//    if(value.eventType == "fail"){
//      val loginState = loginEventIter.iterator()
//      //判断里面是否有值 如果有值 说明已经登陆失败过
//      if(loginState.hasNext){
//        val loginEventState: LoginEvent = loginState.next()
//        if(value.eventTime < loginEventState.eventTime + maxFailCount){
//          // 如果两次间隔小于2秒，输出报警
//          out.collect( Warning( value.userId, loginEventState.eventTime, value.eventTime, "login fail in 2 seconds." ) )
//        }
//        // 更新最近一次的登录失败事件，保存在状态里
//        loginFailState.clear()
//        loginFailState.add(value)
//      }else{
//        //第一次登陆失败  保存在状态里
//        loginFailState.add(value)
//      }
//    }else{
//      //如果登陆成功  清空状态
//      loginFailState.clear()
//    }
//  }
//}
//
