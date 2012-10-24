package com.tehasdf.sstable

import java.io.File
import java.io.DataInputStream
import java.io.FileInputStream
import scala.collection.JavaConversions._
import org.xerial.snappy.Snappy
import com.tehasdf.sstable.input.SeekableDataInputStream
import java.io.ByteArrayInputStream

case class Row(key: String, data: Array[Byte])
class CompressedDataReader(data: SeekableDataInputStream) extends Iterator[Row] {
  def hasNext = data.position < data.length
  def next() = {
    val temp = (data.readByte() & 0xFF) << 8
    val length = temp | (data.readByte() & 0xFF)

    val keyBuf = new Array[Byte](length)
    data.readFully(keyBuf)

    // TODO: Could be an int, make configurable
    val dataLength = data.readLong()

    val dataBuf = new Array[Byte](dataLength.toInt)
    data.readFully(dataBuf)
    Row(new String(keyBuf, "UTF-8"), dataBuf)
  }
}