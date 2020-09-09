package in.gogoi.ds.fixedwidth.util

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

class CustomSerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = FixedWidthUtils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = FixedWidthUtils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}
