package com.tehasdf.sstable

import java.io.{DataInputStream, InputStream}

object ColumnReader {
  private[ColumnReader] val DeletionMask = 0x01
  private[ColumnReader] val ExpirationMask = 0x02
  private[ColumnReader] val CounterMask = 0x04
  private[ColumnReader] val CounterUpdateMask = 0x08
}

sealed trait ColumnState {
  def timestamp: Long
}
case class Column(name: Array[Byte], data: Array[Byte], timestamp: Long) extends ColumnState
case class Deleted(name: Array[Byte], timestamp: Long) extends ColumnState
case class Expiring(name: Array[Byte], data: Array[Byte], ttl: Int, expiration: Int, timestamp: Long) extends ColumnState

class ColumnReader(is: InputStream) extends Iterator[ColumnState] {
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
    val strippedName = nameBuf.reverse.dropWhile( _ == 0x00 ).reverse
    val bitField = data.readUnsignedByte()
    val rv: ColumnState = if ((bitField & CounterMask) != 0) {
      println("this is a counter")
      data.skipBytes(8)
      val cnt = data.readInt()
      data.skipBytes(cnt)
      null
    } else if ((bitField & ExpirationMask) != 0) {
      val ttl = data.readInt()
      val expiration = data.readInt()
      val timestamp = data.readLong()
      val length = data.readInt()
      val buf = new Array[Byte](length)
      data.readFully(buf)
      Expiring(nameBuf, buf, ttl, expiration, timestamp)
    } else {
      val timestamp = data.readLong()
      val length = data.readInt()
      val buf = new Array[Byte](length)
      data.readFully(buf)

      val strippedBuf = buf.reverse.dropWhile( _ == 0x00 ).reverse
      if ((bitField & CounterUpdateMask) != 0) {
        null
      } else if ((bitField & DeletionMask) != 0) {
        Deleted(strippedName, timestamp)
      } else {
        Column(strippedName, strippedBuf, timestamp)
      }
    }

    currentColumn += 1
    rv
  }
}