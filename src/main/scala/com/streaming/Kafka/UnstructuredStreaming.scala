package com.streaming.Kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object UnstructuredStreaming {
  def checkJSON(s: String): Boolean = {

    val jsonString = scala.util.parsing.json.JSON.parseFull(s)
    if (jsonString == None)
      false
    else
      true

  }
  def main(args: Array[String]) {
    val conf = new SparkConf();
    conf.setAppName("Spark MultipleContest Test");
    conf.set("spark.driver.allowMultipleContexts", "true");
    conf.setMaster("local");
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val jsonRdd = stream.filter(x => checkJSON(x.value))
    val nonJsonRdd = stream.filter(x => { !(checkJSON(x.value)) })
jsonRdd.map(f=> f.value()).saveAsTextFiles("/home/rajesh/Desktop/Json", "txt")
    
nonJsonRdd.map(f=> f.value()).saveAsTextFiles("/home/rajesh/Desktop/nonJson", "txt")
ssc.start()
ssc.awaitTermination()
  }
}