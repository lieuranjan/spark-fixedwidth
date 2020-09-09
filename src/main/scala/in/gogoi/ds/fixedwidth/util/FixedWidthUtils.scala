package in.gogoi.ds.fixedwidth.util

import java.io.IOException

import in.gogoi.ds.fixedwidth.FixedWidthOptions
import lombok.extern.slf4j.Slf4j
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{length, trim}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

@Slf4j
object FixedWidthUtils {
  private val logger = LoggerFactory.getLogger(FixedWidthUtils.getClass)
  /**
   * Filter ignorable rows for fixedWidth dataset (lines empty and starting with `comment`).
   * This is currently being used in fixedWidth schema inference.
   */
  def filterCommentAndEmpty(lines: Dataset[String], options: FixedWidthOptions): Dataset[String] = {
    import lines.sqlContext.implicits._
    val nonEmptyLines = lines.filter(length(trim($"value")) > 0)
    if (options.isCommentSet) {
      nonEmptyLines.filter(!$"value".startsWith(options.comment.toString))
    } else {
      nonEmptyLines
    }
  }

  /**
   * Filter ignorable rows for fixedWidth iterator (lines empty and starting with `comment`).
   * This is currently being used in fixedWidth reading path and fixedWidth schema inference.
   */
  def filterCommentAndEmpty(iter: Iterator[String], options: FixedWidthOptions): Iterator[String] = {
    iter.filter { line =>
      line.trim.nonEmpty && !line.startsWith(options.comment.toString)
    }
  }

  /**
   * Skip the given first line so that only data can remain in a dataset.
   * This is similar with `dropHeaderLine` below and currently being used in fixedWidth schema inference.
   */
  def filterHeaderLine(
                        iter: Iterator[String],
                        firstLine: String,
                        options: FixedWidthOptions): Iterator[String] = {
    // Note that unlike actual fixedWidth reading path, it simply filters the given first line. Therefore,
    // this skips the line same with the header if exists. One of them might have to be removed
    // in the near future if possible.
    if (options.headerFlag) {
      iter.filterNot(_ == firstLine)
    } else {
      iter
    }
  }

  def skipComments(iter: Iterator[String], options: FixedWidthOptions): Iterator[String] = {
    if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.dropWhile { line =>
        line.trim.isEmpty || line.trim.startsWith(commentPrefix)
      }
    } else {
      iter.dropWhile(_.trim.isEmpty)
    }
  }

  /**
   * Extracts header and moves iterator forward so that only data remains in it
   */
  def extractHeader(iter: Iterator[String], options: FixedWidthOptions): Option[String] = {
    val nonEmptyLines = skipComments(iter, options)
    if (nonEmptyLines.hasNext) {
      Some(nonEmptyLines.next())
    } else {
      None
    }
  }

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   */
  @throws[IllegalArgumentException]
  def toChar(str: String): Char = {
    (str: Seq[Char]) match {
      case Seq() => throw new IllegalArgumentException("Delimiter cannot be empty string")
      case Seq('\\') => throw new IllegalArgumentException("Single backslash is prohibited." +
        " It has special meaning as beginning of an escape sequence." +
        " To get the backslash character, pass a string with two backslashes as the delimiter.")
      case Seq(c) => c
      case Seq('\\', 't') => '\t'
      case Seq('\\', 'r') => '\r'
      case Seq('\\', 'b') => '\b'
      case Seq('\\', 'f') => '\f'
      // In case user changes quote char and uses \" as delimiter in options
      case Seq('\\', '\"') => '\"'
      case Seq('\\', '\'') => '\''
      case Seq('\\', '\\') => '\\'
      case _ if str == """\u0000""" => '\u0000'
      case Seq('\\', _) =>
        throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      case _ =>
        throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
    }
  }

  /**
   * Sample fixedWidth dataset as configured by `samplingRatio`.
   */
  def sample(ds: Dataset[String], options: FixedWidthOptions): Dataset[String] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      ds
    } else {
      ds.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  /**
   * Sample fixedWidth RDD as configured by `samplingRatio`.
   */
  def sample(ds: RDD[Array[String]], options: FixedWidthOptions): RDD[Array[String]] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) {
      ds
    } else {
      ds.sample(withReplacement = false, options.samplingRatio, 1)
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("IOException encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
  def logError(msg: => String, throwable: Throwable) {
    logger.error(msg, throwable)
  }
}
