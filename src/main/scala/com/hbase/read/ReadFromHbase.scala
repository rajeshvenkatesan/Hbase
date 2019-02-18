package com.hbase.read
import scala.util.Try
import scala.util.parsing.json.JSON

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.hbase.utils.Constants
import com.hbase.utils.Exceptions
import org.apache.log4j.Logger

/**
 * @author rajesh
 * 
 *
 */
object ReadFromHbase extends Constants {
  private val logger = Logger.getLogger(this.getClass().getName());
  def getHbaseConfig(): Configuration = {

    val conf = HBaseConfiguration.create();

    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
    conf.set("hbase.zookeeper.property.clientPort", HBASE_CLIENT_PORT)
    logger.info("Hbase Config Object created")
    conf
  }
  def convertScanToString(scan: Scan) = {
    try {
      val proto = ProtobufUtil.toScan(scan);
      logger.info("Scanner Object converted to String")
      Base64.encodeBytes(proto.toByteArray());
    } catch {
      case e: Exception => {
        throw Exceptions.ConvertScanToStringException()
      }
    }
  }

  def convertArrayOfBytesToString(bytes: Array[Byte]): String = {
    try {
      if (bytes != null)
        bytes.map(_.toChar).mkString
      else
        ""
    } catch {
      case e: Exception => {
        throw Exceptions.BytesConverterException()
      }

    }
  }

  def parseJson(json: String, jsonKey: String): String = {
    try {

      if (Try(JSON.parseFull(json).get.asInstanceOf[Map[String, String]]).isFailure)
        return ""
      val jsonObject = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
      if (jsonKey.contains(",")) {
        val keys = jsonKey.split(',')
        if (Try(jsonObject.get(keys.head).getOrElse("0").asInstanceOf[Map[String, String]]).isFailure)
          return ""
        var baseMapper = jsonObject.get(keys.head).getOrElse("0").asInstanceOf[Map[String, String]]
        val restKeys = keys.drop(1).mkString(",").split(",")
        var resultantColumn = ""

        restKeys.foreach(f => {
          logger.info("JSON column is parsed")
          if (Try(baseMapper.get(f).getOrElse("0").asInstanceOf[Map[String, String]]).isSuccess) {
            baseMapper = baseMapper.get(f).getOrElse("0").asInstanceOf[Map[String, String]]
            resultantColumn = baseMapper.toString()
          } else {
            resultantColumn = baseMapper.get(f).getOrElse("")
          }
        })
        resultantColumn

      } else {
        jsonObject.getOrElse(jsonKey, "")
      }
    } catch {
      case e: Exception => {
        throw Exceptions.JsonParserException()
      }
    }
  }
  def readFromHBase(resultObject: Result, columnFamily: String, columnQualifierName: String): String = {
    try {
      val bytesArray = resultObject.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifierName))
      logger.info("Column Family and Column qualifier are read from Result Object")
      convertArrayOfBytesToString(bytesArray)
    } catch {
      case e: Exceptions.BytesConverterException => {
        throw Exceptions.BytesConverterException()
      }
      case e: Exception => {
        throw Exceptions.HbaseReadException()
      }
    }
  }
  def readRowKey(resultObject: Result): String = {
    try {
      convertArrayOfBytesToString(resultObject.getRow)
    } catch {
      case e: Exceptions.BytesConverterException => {
        throw Exceptions.BytesConverterException()
      }
      case e: Exception => {
        throw Exceptions.RowKeyException()
      }
    }
  }
  def typeConverter(source: String, dataType: String): Any = {
    try {
      logger.info("typeConverter() starts")
      val resultantColumn = dataType match {
        case "double" => {
          if (Try(source.toDouble).isSuccess)
            source.toDouble
          else
            0.0
        }
        case "int" => {
          if (Try(source.toInt).isSuccess)
            source.toInt
          else
            0
        }
        case _ => {
          source
        }

      }
      resultantColumn
    } catch {
      case e: Exception => {
        throw Exceptions.TypeConverterException()
      }
    }
  }
  def scanTablesOnTimestamp(spark: SparkSession, conf: Configuration, tableName: String,
                            columnsToRead: Seq[Map[String, String]], minTimeStamp: Long, maxTimeStamp: Long): org.apache.spark.sql.DataFrame = {
    try {
      conf.set(TableInputFormat.INPUT_TABLE, tableName)

      val scan = new Scan();
      scan.setCaching(1000);
      scan.setCacheBlocks(false);
      scan.setTimeRange(minTimeStamp, maxTimeStamp)
      conf.set(TableInputFormat.SCAN, convertScanToString(scan))
      val rdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      val res: RDD[Row] = rdd.map(f => f._2).map(f => {
        Row(columnsToRead.map(g => {
          val datatype = g.getOrElse("datatype", "string")
          g.getOrElse("type", "").toLowerCase() match {
            case "rowkey" => {
              readRowKey(f)
              logger.info("Row Key is determined")
            }
            case "json" => {
              val columnFamily = g.getOrElse("columnFamily", "")
              val columnQualifierName = g.getOrElse("columnQualifier", "")
              val jsonKey = g.getOrElse("hierarchy", "")
              val result = readFromHBase(f, columnFamily, columnQualifierName)

              val parsedJsonValue = parseJson(result, jsonKey)
              logger.info("JSON message is beingin scan parsed in scanTablesOnTimestamp()")
              typeConverter(parsedJsonValue, datatype)
            }
            case _ => {
              val columnFamily = g.getOrElse("columnFamily", "")
              val columnQualifierName = g.getOrElse("columnQualifier", "")
              val result = readFromHBase(f, columnFamily, columnQualifierName)

              typeConverter(result, datatype)
            }
          }

        }))

      })
      val datatypes = columnsToRead.map(f => f.get("datatype").getOrElse("string"))
      val aliasNames = columnsToRead.map(f => f.get("aliasName").getOrElse("string"))

      val schema = schemaGenerator(datatypes, aliasNames)
      val res1 = res.map(f => f(0).asInstanceOf[Seq[String]]).map(f => Row.fromSeq(f))

      val resultDataFrame = spark.sqlContext.createDataFrame(res1, schema)

      resultDataFrame
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    spark.sqlContext.emptyDataFrame
  }

  def schemaGenerator(datatypes: Seq[String], aliasNames: Seq[String]): StructType = {
    try {

      val schema = StructType(datatypes.zip(aliasNames).map(f => {
        f._1.toLowerCase() match {
          case "double" => StructField(f._2, DoubleType, true)
          case "int"    => StructField(f._2, IntegerType, true)
          case _        => StructField(f._2, StringType, true)
        }
      }))
      schema
    } catch {
      case e: Exception => {
        throw Exceptions.SchemaGeneratorException()
      }
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()
    val conf = getHbaseConfig
    val minTimestamp: Long = 1549813804501L
    val maxTimestamp: Long = 1550401503338L
    val mapper = Map("columnFamily" -> "i", "columnQualifier" -> "productImage", "aliasName" -> "Image", "type" -> "string", "datatype" -> "string", "hierarchy" -> "")
    val mapper1 = Map("columnFamily" -> "i", "columnQualifier" -> "value", "type" -> "json", "aliasName" -> "relevance", "datatype" -> "double", "hierarchy" -> "relevance")
    val mapper2 = Map("columnFamily" -> "i", "columnQualifier" -> "value", "type" -> "json", "aliasName" -> "relevance12", "datatype" -> "string", "hierarchy" -> "root,a,b")
    val rowKey = Map("type" -> "rowKey", "aliasName" -> "RowKey")
    val columnsToRead = Seq(mapper, mapper1, mapper2, rowKey)

    val df = scanTablesOnTimestamp(spark, conf, "emp", columnsToRead, minTimestamp, maxTimestamp)
    df.show

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
}