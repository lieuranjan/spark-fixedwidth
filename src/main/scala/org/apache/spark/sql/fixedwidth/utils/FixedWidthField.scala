package org.apache.spark.sql.fixedwidth.utils

case class FixedWidthField(
                            name: String,
                            length: Int,
                            startPosition: Int,
                            endPosition: Int,
                            padding: String = "\u0000",
                            alignment: String = "left"
                          )
