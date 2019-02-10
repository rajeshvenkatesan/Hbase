package com.hbase.write
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.hbase.utils.Constants

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.protobuf.ProtobufUtil

object ReadFromHbase extends Constants {
  def getHbaseConfig(): Configuration = {
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
    conf.set("hbase.zookeeper.property.clientPort", HBASE_CLIENT_PORT)
   
    conf
  }
def convertScanToString(scan : Scan) = {
      val proto = ProtobufUtil.toScan(scan);
      Base64.encodeBytes(proto.toByteArray());
  }

  def convertArrayOfBytesToString(bytes: Array[Byte]) = {
    bytes.filter(p => p != null).map(_.toChar).mkString
  }
  def scanTablesOnTimestamp(spark: SparkSession, conf: Configuration, tableName: String,
      columnsToRead: Seq[String], dataFrameAliasName: Seq[String]):org.apache.spark.sql.DataFrame={
    
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

        val scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        scan.setTimeRange(1549813804501L, 1549813804502L)
       conf.set(TableInputFormat.SCAN, convertScanToString(scan))
     val rdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])  
   val res= rdd.map(f=>f._2).map(f=>{
  Row(columnsToRead.map(g=>{
       val columnFamily=g.split(":")(0)
       val columnQualifierName=g.split(":")(1)
       f.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifierName))
       
     }))
     
   })
    val schema = StructType(dataFrameAliasName.map(fieldName ⇒ StructField(fieldName, StringType, true)))
   val resultDataFrame = spark.sqlContext.createDataFrame(res, schema)   
   resultDataFrame
  }
  def fullScanTable(spark: SparkSession, conf: Configuration, tableName: String,
                    rowKeys: Seq[String], columnsToRead: Seq[String], dataFrameAliasName: Seq[String]): org.apache.spark.sql.DataFrame = {

    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin = new HBaseAdmin(conf)
    var columnsReadForAllRowKeys = Array[Row]()
    if (admin.isTableAvailable(tableName)) {

      val table = connection.getTable(TableName.valueOf(tableName))

      for (rowKey <- rowKeys) {
        var columnsReadForRowKey = Array[String]()
        val hbaseRow = table.get(new Get(Bytes.toBytes(rowKey)))

        columnsToRead.foreach(f => {
          val columnFamily = f.split(":")(0)
          val columnQualifierName = f.split(":")(1)
          val arrayOfBytes: Array[Byte] = hbaseRow.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifierName))

          val column = convertArrayOfBytesToString(arrayOfBytes)
          columnsReadForRowKey = columnsReadForRowKey :+ column
        })

        columnsReadForAllRowKeys = columnsReadForAllRowKeys :+ Row(columnsReadForRowKey: _*)
      }

    }
    val schema = StructType(dataFrameAliasName.map(fieldName ⇒ StructField(fieldName, StringType, true)))
    val resultDataFrame = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(columnsReadForAllRowKeys), schema)
    resultDataFrame
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()
    val conf = getHbaseConfig
  val rowKeys = Seq("7111689861")
    val columnsToRead = Seq("i:value", "i:productImage")
    val dataFrameAliasName = Seq("valuesss", "ProductImage")
 /*   val result = fullScanTable(spark, conf, "emp1", rowKeys, columnsToRead, dataFrameAliasName)
    result.show*/
val df=scanTablesOnTimestamp(spark, conf, "emp",columnsToRead,dataFrameAliasName)
df.show
  }
}