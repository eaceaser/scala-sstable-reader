package com.tehasdf.sstable

import java.io.File
import java.io.DataInputStream
import java.io.FileInputStream
import scala.collection.JavaConversions._
import org.xerial.snappy.Snappy
import com.tehasdf.sstable.input.SeekableDataInputStream
import java.io.ByteArrayInputStream

case class Row(key: String, columns: ColumnReader)
class DataReader(data: SeekableDataInputStream) extends Iterator[Row] {
  private def readKeyLength() = {
    val temp = (data.readByte() & 0xFF) << 8
    temp | (data.readByte() & 0xFF)
  }

  def hasNext = {
    if (data.remaining < 2) {
      false
    } else {
      val pos = data.position
      val len = readKeyLength()
      if (data.remaining < len) {
        false
      } else {
        data.skipBytes(len)
        if (data.remaining < 8) {
          false
        } else {
          val dataLength = data.readLong()
          if (data.remaining < dataLength) {
            false
          } else {
            data.seek(pos)
            true
          }
        }
      }
    }
  }

  def next() = {
    val length = readKeyLength()
    val keyBuf = new Array[Byte](length)
    data.readFully(keyBuf)

    // TODO: Could be an int, make configurable
    val dataLength = data.readLong()

    println("reading a key of len %d w/ len : %d".format(length, dataLength))
    val dataBuf = new Array[Byte](dataLength.toInt)
    data.readFully(dataBuf)

    Row(new String(keyBuf, "UTF-8"), new ColumnReader(new ByteArrayInputStream(dataBuf)))
  }
}