package com.jh.flinkHotItemanalyis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

/**
  * create by young on 2020.7.22 
  * desc: 
  */
object HotItemsProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "bqy01:9092,bqy02:9092,bqy03:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)

    //scala io  从本地读取
    val source = Source.fromFile("E:\\ideaflinkHotItem\\flinkHotItemanaly\\src\\main\\resources\\UserBehavior.csv")
    for(line <- source.getLines()){
//      println(line)
      Thread.sleep(500)
      producer.send(new ProducerRecord[String,String]("test02",line))

    }
  }

}
