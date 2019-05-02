package com.streaming.Kafka

import org.apache.spark.sql.SparkSession
object ParQuetReader {
   def main(args: Array[String]) {
      val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()
    import spark.sqlContext._
  val jsonDf = spark.read.parquet("/home/rajesh/Desktop/JSON")
  jsonDf.show
val nonJsonDf = spark.read.parquet("/home/rajesh/Desktop/NotJSON")
  nonJsonDf.show
  
  

   }
}