package com.fyj.spark_kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {

    val Array(zkQuorm,group,topics,numThreads) = args
    //(zk hostname:port,consumer groupId,topics,线程数)

    val sc = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(sc,Seconds(5))

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val kafkaStream = KafkaUtils.createStream(ssc,zkQuorm,group,topicMap)
    //进行wordcount统计
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
