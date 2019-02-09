package com.hbase.write

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.get_json_object

object WriteToHbase {
  def rowKeyCreation(df: org.apache.spark.sql.Dataset[String], rowKeys: Seq[String], json: Seq[String]) = {
    var temp = df.toDF

    for ((i, j) <- rowKeys.zip(json)) {
      var f = "$." + j
      temp = temp.withColumn(i, get_json_object(col("value"), f))
    }
    temp
  }
  def convert(row: org.apache.spark.sql.Row, rowKeys: Seq[String], columnsFromDataFrame: Seq[String], columnFamily: Seq[String]): (ImmutableBytesWritable, Put) = {
    var rowKey: StringBuilder = new StringBuilder("")
    for (i <- rowKeys) {
      if (row.getAs(i) != null)
        rowKey.append(row.getAs(i).toString)

    }

    val p = new Put(Bytes.toBytes(rowKey.toString()))
    val columnStorage = columnsFromDataFrame.zip(columnFamily)

    columnStorage.foreach(f => {
      if (f._2.split(":").length == 2 && row.getAs(f._1) != null)
        p.add(Bytes.toBytes(f._2.split(":")(0)), Bytes.toBytes(f._2.split(":")(1)), Bytes.toBytes(row.getAs(f._1).toString))

    })

    (new ImmutableBytesWritable, p)
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()
    val df = spark.read.format("csv").option("header", "true").option("delimiter", "|")

      .load("data.csv")

    val df1 = df.filter(f => !f.anyNull)
      .filter(f => {
        f.getAs("_unit_id").toString.forall(_.isDigit)
      })
    val res = df1.toJSON

    val rowKeysmatchingJson = Seq("_unit_id", "relevance")
    val rowKeys = Seq("rowKey1", "rowKey2")
    val columnsFromDataFrame = Seq("value")
    val columnFamily = Seq("i:value")
    val hbaseDf = rowKeyCreation(res, rowKeys, rowKeysmatchingJson)

    val conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "emp")

    val hbaseConfig = new JobConf(conf)
    hbaseConfig.setOutputFormat(classOf[TableOutputFormat])

    hbaseDf.filter(f => !f.anyNull).rdd.map(f => convert(f, rowKeys, columnsFromDataFrame, columnFamily)).saveAsHadoopDataset(hbaseConfig)

  }
}