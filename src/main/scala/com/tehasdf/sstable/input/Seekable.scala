package com.tehasdf.sstable.input

import java.io.DataInput
import java.io.IOException

trait Seekable {
  def seek(to: Long): Unit
  def position: Long
  def length: Long
  def remaining = length - position
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

class BoundedSeekableDataInputStreamProxy(inner: SeekableDataInputStream, start: Long, val length: Long) extends SeekableDataInputStream {
  inner.seek(start)
  private val end = start + length
  private def consumeBytes[A](num: Int)(f: SeekableDataInputStream => A): A = {
    if ((position + num) > length) throw new IOException("Out of bounds.")
    f(inner)
  }
  
  def position = inner.position - start
  def seek(to: Long) {
    if (to > length) throw new IOException("Byte offset: %d is outside of the bounds of this seekable %d")
    inner.seek(to)
  }
  
  def readBoolean() = consumeBytes(1) { _.readBoolean() }
  def readByte() = consumeBytes(1) { _.readByte() }
  def readChar() = consumeBytes(2) { _.readChar() }
  def readDouble() = consumeBytes(8) { _.readDouble() }
  def readFloat() = consumeBytes(4) { _.readFloat() }
  def readFully(buf: Array[Byte]) = consumeBytes(buf.length) { _.readFully(buf) }
  def readFully(buf: Array[Byte], offset: Int, length: Int) = consumeBytes(length) { _.readFully(buf, offset, length) }
  def readInt() = consumeBytes(4) { _.readInt() }
  def readLine() = { throw new Exception("NOt implemented") }
  def readLong() = consumeBytes(8) { _.readLong() }
  def readShort() = consumeBytes(2) { _.readShort() }
  def readUnsignedByte() = consumeBytes(1) { _.readUnsignedByte() }
  def readUnsignedShort() = consumeBytes(2) { _.readUnsignedShort() }
  def readUTF() = { throw new Exception("Not implemented") }
  def skipBytes(n: Int) = consumeBytes(n) { _.skipBytes(n )}
}