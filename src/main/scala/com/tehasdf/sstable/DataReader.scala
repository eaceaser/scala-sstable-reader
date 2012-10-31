package com.tehasdf.sstable

import com.tehasdf.sstable.input.SeekableDataInputStream

import java.io.{ByteArrayInputStream, EOFException}

case class Row(key: String, columns: ColumnReader)

class DataReader(data: SeekableDataInputStream) extends Iterator[Row] {
  private def readKeyLength() = {
    val temp = (data.readByte() & 0xFF) << 8
    temp | (data.readByte() & 0xFF)
  }

  // yuck.. state! prevents multiple passes though.
  var currentKey: Array[Byte] = null
  var currentData: Array[Byte] = null


  def hasNext = {
    if (data.remaining < 2) {
      currentKey = null; currentData = null
      false
    } else {
      val pos = data.position
      val len = readKeyLength()
      if (data.remaining < len) {
        currentKey = null; currentData = null
        false
      } else {
        currentKey = new Array[Byte](len)
        data.readFully(currentKey)
        if (data.remaining < 8) {
          currentKey = null; currentData = null
          false
        } else {
          val dataLength = data.readLong()
          if (data.remaining < dataLength) {
            currentKey = null; currentData = null
            false
          } else {
            currentData = new Array[Byte](dataLength.toInt)
            data.readFully(currentData)
            true
          }
        }
      }
    }
  }

  def next() = {
    if (currentKey == null || currentData == null) throw new EOFException("Attempted to read a row after the end of the data stream.")
    Row(new String(currentKey, "UTF-8"), new ColumnReader(new ByteArrayInputStream(currentData)))
  }
}