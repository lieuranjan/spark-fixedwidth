package org.apache.spark.sql.fixedwidth

import java.nio.charset.StandardCharsets
import java.util.{Locale, TimeZone}
import com.univocity.parsers.fixed.{FieldAlignment, FixedWidthFields, FixedWidthParserSettings, FixedWidthWriterSettings}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/**
 * @author lieuranjan
 *         Created on 12/9/20
 */
class FixedWidthOptions(
                         @transient val parameters: CaseInsensitiveMap[String],
                         val columnPruning: Boolean,
                         defaultTimeZoneId: String,
                         defaultColumnNameOfCorruptRecord: String)
  extends Logging with Serializable {

  def this(
            parameters: Map[String, String],
            columnPruning: Boolean,
            defaultTimeZoneId: String,
            defaultColumnNameOfCorruptRecord: String = "") = this(
    CaseInsensitiveMap(parameters),
    columnPruning,
    defaultTimeZoneId,
    defaultColumnNameOfCorruptRecord)

  private def getChar(paramName: String, default: Char) = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  private def getInt(paramName: String, default: Int) = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try value.toInt catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }

  private def getBool(paramName: String, default: Boolean = false) = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) default else if (param.toLowerCase(Locale.ROOT) == "true") true else if (param.toLowerCase(Locale.ROOT) == "false") false else throw new Exception(s"$paramName flag can be true or false")
  }

  val fieldLengths = parameters.getOrElse("fieldLengths", "")
  val fieldSchema = parameters.getOrElse("fieldSchema", "")

  val padding = CSVUtils.toChar(parameters.getOrElse("padding", " "))
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val charset = parameters.getOrElse("encoding",
    parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))

  val comment = getChar("comment", '\u0000')

  val headerFlag = getBool("header")
  val inferSchemaFlag = getBool("inferSchema")
  val ignoreLeadingWhiteSpaceInRead = getBool("ignoreLeadingWhiteSpace", default = false)
  val ignoreTrailingWhiteSpaceInRead = getBool("ignoreTrailingWhiteSpace", default = false)

  // For write, both options were `true` by default. We leave it as `true` for
  // backwards compatibility.
  val ignoreLeadingWhiteSpaceFlagInWrite = getBool("ignoreLeadingWhiteSpace", default = true)
  val ignoreTrailingWhiteSpaceFlagInWrite = getBool("ignoreTrailingWhiteSpace", default = true)

  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)
  val nullValue = parameters.getOrElse("nullValue", "")

  val nanValue = parameters.getOrElse("nanValue", "NaN")

  val positiveInf = parameters.getOrElse("positiveInf", "Inf")
  val negativeInf = parameters.getOrElse("negativeInf", "-Inf")

  val compressionCodec: Option[String] = {
    val name = parameters.get("compression").orElse(parameters.get("codec"))
    name.map(CompressionCodecs.getCodecClassName)
  }

  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)
  val maxColumns = getInt("maxColumns", 20480)
  val maxCharsPerColumn = getInt("maxCharsPerColumn", -1)
  val inputBufferSize = 128
  val isCommentSet = this.comment != '\u0000'
  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

  /**
   * Forcibly apply the specified or inferred schema to datasource files.
   * If the option is enabled, headers of text files will be ignored.
   */
  val enforceSchema = getBool("enforceSchema", default = true)

  val lineSeparator = parameters.getOrElse("lineSeparator", "\n")
  /**
   * String representation of an empty value in read and in write.
   */
  val emptyValue = parameters.get("emptyValue")
  /**
   * The string is returned when txt reader doesn't have any characters for input value,
   * or an empty quoted string `""`. Default value is empty string.
   */
  val emptyValueInRead = emptyValue.getOrElse("")
  /**
   * The value is used instead of an empty string in write. Default value is `""`
   */
  val emptyValueInWrite = emptyValue.getOrElse("\"\"")
  /**
   * Output file extension , default is txt
   */
  val extension = parameters.getOrElse("extension", "txt").trim

  def asWriterSettings: FixedWidthWriterSettings = {
    val fields = getFixedWidthFields(fieldLengths, fieldSchema)
    val writerSettings = new FixedWidthWriterSettings(fields)
    val format = writerSettings.getFormat
    format.setPadding(padding)
    format.setComment(comment)
    writerSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlagInWrite)
    writerSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlagInWrite)
    writerSettings.setNullValue(nullValue)
    writerSettings.setEmptyValue(emptyValueInWrite)
    writerSettings.setSkipEmptyLines(true)
    writerSettings.setHeaderWritingEnabled(headerFlag)
    writerSettings.setUseDefaultPaddingForHeaders(true)
    writerSettings.setDefaultAlignmentForHeaders(FieldAlignment.LEFT)
    writerSettings
  }

  def asParserSettings: FixedWidthParserSettings = {
    val fields = getFixedWidthFields(fieldLengths, fieldSchema)
    val settings = new FixedWidthParserSettings(fields)
    val format = settings.getFormat
    format.setComment(comment)
    format.setPadding(padding)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceInRead)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceInRead)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(inputBufferSize)
    settings.setMaxColumns(maxColumns)
    settings.setNullValue(nullValue)
    settings.setMaxCharsPerColumn(maxCharsPerColumn)
    settings.setSkipEmptyLines(true)
    settings.setLineSeparatorDetectionEnabled(true)
    //columns
    settings
  }

  case class Field(
                    name: Option[String],
                    length: Option[Int],
                    startPosition: Option[Int],
                    endPosition: Option[Int],
                    padding: Option[String],
                    alignment: Option[String]
                  )

  //creating FixedWidthFields from lengths or FieldSchema
  private def getFixedWidthFields(lengths: String, fieldSchema: String) = {
    val fixedWidthFields = new FixedWidthFields()
    if (!lengths.isEmpty) {
      val lengthArray = lengths.split(",").map(r => r.toInt)
      lengthArray.foreach(l => fixedWidthFields.addField(l))
      fixedWidthFields
    } else if (!fieldSchema.isEmpty) {
      implicit val formats = DefaultFormats
      println(fieldSchema)
      val fields = read[List[Field]](fieldSchema)
      println(fields)
      fields.foreach(field => {
        if (field.length.isEmpty) {
          fixedWidthFields.addField(field.name.get, field.startPosition.get, field.endPosition.get, toFieldAlignment(field.alignment), getFieldPadding(field.padding))
        } else {
          fixedWidthFields.addField(field.name.get, field.length.get, toFieldAlignment(field.alignment), getFieldPadding(field.padding))
        }
      })
      fixedWidthFields
    } else
      throw new IllegalArgumentException("Expected fieldLength/fieldSchema to parse/write fixedwidth files")
  }

  private def toFieldAlignment(alignment: Option[String]): FieldAlignment = {
    alignment.get match {
      case "left" => FieldAlignment.LEFT
      case "right" => FieldAlignment.RIGHT
      case "center" => FieldAlignment.CENTER
      case _ => FieldAlignment.LEFT
    }
  }

  private def getFieldPadding(padding: Option[String]): Char = {
    if (padding.get.isEmpty) {
      '\u0000'
    } else {
      CSVUtils.toChar(padding.get)
    }
  }
}

