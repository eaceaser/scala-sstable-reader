package com.tehasdf.sstable.input

import java.io.File
import java.io.FileInputStream
import java.io.DataInputStream

class FileSeekableDataInputStream(file: File) extends SeekableDataInputStream {
  val fis = new FileInputStream(file)
  val dis = new DataInputStream(fis)
  val chan = fis.getChannel()
  
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
  
  def seek(to: Long) { chan.position(to) }
  def position = chan.position()
  val length = file.length()
}