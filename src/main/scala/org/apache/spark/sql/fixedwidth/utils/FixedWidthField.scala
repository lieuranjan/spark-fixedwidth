package org.apache.spark.sql.fixedwidth.utils

/**
 * @author lieuranjan
 *         Created on 12/9/20
 */
case class FixedWidthField(
                            name: String,
                            length: Int,
                            startPosition: Int,
                            endPosition: Int,
                            padding: String = "\u0000",
                            alignment: String = "left"
                          )
