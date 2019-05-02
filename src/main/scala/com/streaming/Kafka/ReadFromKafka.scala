package com.streaming.Kafka

import org.apache.spark.sql.SparkSession

object ReadFromKafka {
  def checkJSON(s: String): Boolean = {

    val jsonString = scala.util.parsing.json.JSON.parseFull(s)
    if (jsonString == None)
      false
    else
      true

  }
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()
    import spark.sqlContext._
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
    //{"menu": {"id": "file","value": "File"}}
    import org.apache.spark.sql.functions._
    val udf = checkJSON _
    val consoleOutput = ds1
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .repartition(1)

      .filter(x => (udf(x.getAs("value"))))
      .writeStream
      .outputMode("append")
      //.format("console")
      .option("path", "/home/rajesh/Desktop/JSON")
      .option("checkpointLocation", "/home/rajesh/Desktop/checkpoint1")

      .start()

    val consoleOutput1 = ds1
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .repartition(1)
      .filter(x => !(udf(x.getAs("value"))))
      .writeStream
      .outputMode("append")
      //.format("console")
      .option("path", "/home/rajesh/Desktop/NotJSON")
      .option("checkpointLocation", "/home/rajesh/Desktop/checkpoint")

      .start()

    consoleOutput1.awaitTermination()
    consoleOutput.awaitTermination()
  }
}