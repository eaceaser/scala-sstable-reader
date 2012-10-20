package com.tehasdf.sstable.input

import java.io.DataInput

trait Seekable {
  def seek(to: Long): Unit
  def position: Long
  def length: Long
}

trait SeekableDataInputStream extends DataInput with Seekable

abstract class SeekableDataInputStreamProxy(dis: DataInput) extends SeekableDataInputStream {
  def readBoolean() = dis.readBoolean()
  def readByte() = dis.readByte()
  def readChar() = dis.readChar()
  def readDouble() = dis.readDouble()
  def readFloat() = dis.readFloat()
  def readFully(b: Array[Byte]) { dis.readFully(b) }
  def readFully(b: Array[Byte], off: Int, len: Int) { dis.readFully(b, off, len) }
  def readInt() = dis.readInt()
  def readLine() = dis.readLine()
  def readShort() = dis.readShort()
  def readLong() = dis.readLong()
  def readUnsignedByte() = dis.readUnsignedByte()
  def readUnsignedShort() = dis.readUnsignedShort()
  def readUTF() = dis.readUTF()
  def skipBytes(n: Int) = dis.skipBytes(n)
}
