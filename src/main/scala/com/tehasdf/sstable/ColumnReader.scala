package com.tehasdf.sstable

import java.io.InputStream
import java.io.DataInputStream

object ColumnReader {
  private[ColumnReader] val DeletionMask = 0x01
  private[ColumnReader] val ExpirationMask = 0x02
  private[ColumnReader] val CounterMask = 0x04
  private[ColumnReader] val CounterUpdateMask = 0x08
}

case class Column(name: Array[Byte], data: Array[Byte])
class ColumnReader(is: InputStream) extends Iterator[Column] {
  import ColumnReader._
  val data = new DataInputStream(is)

  // skip bloom filter
  val bloomSize = data.readInt()
  data.skipBytes(bloomSize)

  // skip Index
  val columnIndexSize = data.readInt()
  data.skipBytes(columnIndexSize)

  val localTime = data.readInt()
  val timestamp = data.readLong()

  val numColumns = data.readInt()
  var currentColumn = 0

  def hasNext = currentColumn < numColumns
  def next() = {
    val colNameLen = data.readUnsignedShort()
    val nameBuf = new Array[Byte](colNameLen)
    data.readFully(nameBuf)

    val bitField = data.readUnsignedByte()
    val dataBuf = if ((bitField & CounterMask) != 0) {
      println("this is a counter")
      data.skipBytes(8)
      val cnt = data.readInt()
      data.skipBytes(cnt)
      Array.empty[Byte]
    } else if ((bitField & ExpirationMask) != 0) {
      println("expiring column, wtf")
      val ttl = data.readInt()
      val expiration = data.readInt()
      val timestamp = data.readLong()
      val length = data.readInt()
      data.skipBytes(length)
      Array.empty[Byte]
    } else {
      val timestamp = data.readLong()
      val length = data.readInt()
      val buf = new Array[Byte](length)
      data.readFully(buf)
      if ((bitField & CounterUpdateMask) != 0) {
        println("stupid")
        Array.empty[Byte]
      } else if ((bitField & DeletionMask) != 0) {
        println("deleted column!")
        Array.empty[Byte]
      } else {
        buf
      }
    }

    currentColumn += 1
    Column(nameBuf, dataBuf)
  }
}