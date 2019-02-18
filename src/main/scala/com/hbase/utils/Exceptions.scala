package com.hbase.utils

/**
 * @author rajesh
 *
 */
object Exceptions {
  val HBASEREAD_EXCEPTION_MESSAGE = "Exception occured when trying to read from Hbase"
  val BYTESCONVERTER_EXCEPTION_MESSAGE = "Exception occured when trying to convert bytes to String"
  val JSONPARSER_EXCEPTION_MESSAGE = "Exception occured when parsing JSON"
  val ROWKEY_EXCEPTION_MESSAGE = "Exception occured when trying to read Row Keys"
  val TYPECONVERTER_EXCEPTION_MESSAGE = "Exception occured when casting the column"
  val CONVERTSCANTOSTRING_EXCEPTION_MESSAGE = "Exception occured when trying to convert Scan Object"
  val SCHEMAGENERATOR_EXCEPTION_MESSAGE = "Exception occured when generating Schema for dataframe"

  case class HbaseReadException(
    private val message: String    = HBASEREAD_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class BytesConverterException(
    private val message: String    = BYTESCONVERTER_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class JsonParserException(
    private val message: String    = JSONPARSER_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class RowKeyException(
    private val message: String    = ROWKEY_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class TypeConverterException(
    private val message: String    = TYPECONVERTER_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class ConvertScanToStringException(
    private val message: String    = CONVERTSCANTOSTRING_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
  case class SchemaGeneratorException(
    private val message: String    = SCHEMAGENERATOR_EXCEPTION_MESSAGE,
    private val cause:   Throwable = None.orNull) extends Exception(message, cause)
}