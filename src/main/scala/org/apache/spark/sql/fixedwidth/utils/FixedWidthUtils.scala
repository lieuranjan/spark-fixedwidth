package org.apache.spark.sql.fixedwidth.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.fixedwidth.FixedWidthOptions
import org.apache.spark.sql.functions.{length, trim}

/**
 * @author lieuranjan
 *         Created on 12/9/20
 */
object FixedWidthUtils {

  /**
   * Filter ignorable rows for fixedWidth dataset (lines empty and starting with `comment`).
   * This is currently being used in fixedWidth schema inference.
   */
  def filterCommentAndEmpty(lines: Dataset[String], options: FixedWidthOptions): Dataset[String] = {
    import lines.sqlContext.implicits._
    val nonEmptyLines = lines.filter(length(trim($"value")) > 0)
    if (options.isCommentSet) nonEmptyLines.filter(!$"value".startsWith(options.comment.toString)) else nonEmptyLines
  }

  /**
   * Filter ignorable rows for fixedWidth iterator (lines empty and starting with `comment`).
   * This is currently being used in fixedWidth reading path and fixedWidth schema inference.
   */
  def filterCommentAndEmpty(iter: Iterator[String], options: FixedWidthOptions): Iterator[String] = iter.filter { line =>
    line.trim.nonEmpty && !line.startsWith(options.comment.toString)
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
    if (options.headerFlag) iter.filterNot(_ == firstLine) else iter
  }

  def skipComments(iter: Iterator[String], options: FixedWidthOptions): Iterator[String] = if (options.isCommentSet) {
    val commentPrefix = options.comment.toString
    iter.dropWhile { line =>
      line.trim.isEmpty || line.trim.startsWith(commentPrefix)
    }
  } else iter.dropWhile(_.trim.isEmpty)

  /**
   * Extracts header and moves iterator forward so that only data remains in it
   */
  def extractHeader(iter: Iterator[String], options: FixedWidthOptions): Option[String] = {
    val nonEmptyLines = skipComments(iter, options)
    if (nonEmptyLines.hasNext) Some(nonEmptyLines.next()) else None
  }

  /**
   * Sample fixedWidth dataset as configured by `samplingRatio`.
   */
  def sample(ds: Dataset[String], options: FixedWidthOptions): Dataset[String] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) ds else ds.sample(withReplacement = false, options.samplingRatio, 1)
  }

  /**
   * Sample fixedWidth RDD as configured by `samplingRatio`.
   */
  def sample(ds: RDD[Array[String]], options: FixedWidthOptions): RDD[Array[String]] = {
    require(options.samplingRatio > 0,
      s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    if (options.samplingRatio > 0.99) ds else ds.sample(withReplacement = false, options.samplingRatio, 1)
  }
}
