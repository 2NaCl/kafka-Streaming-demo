package com.fyj.spark_kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectApp {
  def main(args: Array[String]): Unit = {

    val Array(brokers,topics) = args


    val sc = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String,String]("metadata.broker.list"->brokers)

    val topicSet = topics.split(",").toSet

    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )
    //进行wordcount统计
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
