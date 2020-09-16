package org.apache.spark.sql.fixedwidth

import java.nio.charset.Charset

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.fixedwidth.univocity.{UnivocityParser, UnivocityWriter}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * @author lieuranjan
 *         Created on 12/9/20
 */
/**
 * Provides access to FixedWidth data from pure SQL statements.
 */
class FixedWidthFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "fixedWidth"

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    val parsedOptions = new FixedWidthOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    val fixedWidthDataSource = FixedWidthDataSource(parsedOptions)
    fixedWidthDataSource.isSplitable && super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new FixedWidthOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

    FixedWidthDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val fixedWidthOptions = new FixedWidthOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    fixedWidthOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new FixedWidthOutputWriter(path, dataSchema, context, fixedWidthOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        fixedWidthOptions.extension + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions = new FixedWidthOptions(
      options,
      sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    /// Check a field requirement for corrupt records here to throw an exception in a driver side
    dataSchema.getFieldIndex(parsedOptions.columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = dataSchema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new AnalysisException(
          "The field for corrupt records must be string type and nullable")
      }
    }

    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new AnalysisException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
          "referenced columns only include the internal corrupt record column\n" +
          s"(named _corrupt_record by default). For example:\n" +
          "spark.read.schema(schema).csv(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
          "and spark.read.schema(schema).csv(file).select(\"_corrupt_record\").show().\n" +
          "Instead, you can cache or save the parsed results and then send the same query.\n" +
          "For example, val df = spark.read.schema(schema).csv(file).cache() and then\n" +
          "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val columnPruning = sparkSession.sessionState.conf.csvColumnPruning

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val parser = new UnivocityParser(
        StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        StructType(requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
        parsedOptions)
      FixedWidthDataSource(parsedOptions).readFile(
        conf,
        file,
        parser,
        requiredSchema,
        dataSchema,
        caseSensitive,
        columnPruning)
    }
  }

  override def toString: String = "fixedWidth"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[FixedWidthFileFormat]

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = dataType match {
    case _: AtomicType => true
    case udt: UserDefinedType[_] => supportDataType(udt.sqlType, isReadPath)
    case _ => false
  }

}

private[fixedwidth] class FixedWidthOutputWriter(
                                                  path: String,
                                                  dataSchema: StructType,
                                                  context: TaskAttemptContext,
                                                  params: FixedWidthOptions) extends OutputWriter with Logging {

  private val charset = Charset.forName(params.charset)

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), charset)

  private val generator = new UnivocityWriter(dataSchema, writer, params)

  override def write(row: InternalRow): Unit = generator.write(row)

  override def close(): Unit = generator.close()
}
