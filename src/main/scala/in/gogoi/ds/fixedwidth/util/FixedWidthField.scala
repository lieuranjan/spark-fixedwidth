package in.gogoi.ds.fixedwidth.util
case class FixedWidthField(
                            name: String,
                            length: Int,
                            startPosition: Int,
                            endPosition: Int,
                            padding: String = "\u0000",
                            alignment: String = "left"
                          )
