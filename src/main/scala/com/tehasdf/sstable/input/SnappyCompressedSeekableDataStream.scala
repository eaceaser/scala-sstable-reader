package com.tehasdf.sstable.input

import java.io.DataInput
import java.io.DataInputStream
import com.tehasdf.sstable.CompressionInfoReader
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

class SnappyCompressedSeekableDataStream(data: SeekableDataInputStream, compressionInfo: CompressionInfoReader) extends SeekableDataInputStream {
  case class Chunk(index: Int, startPosition: Long, data: Array[Byte], inputStream: ByteArrayInputStream, dataInputStream: DataInputStream)

  def length = data.length
  def position = currentChunk.startPosition + (currentChunk.data.length - currentChunk.inputStream.available())
  def seek(to: Long) = {
    throw new Exception("OMFG NOT IMPLEMENTED !!!")
  }

  private var currentOffset = 0
  private val offsets = compressionInfo.toList
  private var currentChunk = readNextChunk()

  private def readNextChunk() = {
    val rv = readChunk(currentOffset)
    currentOffset += 1
    rv
  }

  private def readChunk(offset: Int) = {
    val chunkPos = offsets(offset)
    val nextChunkPos = offsets.lift(offset+1).getOrElse(data.length)
    val len = (nextChunkPos - chunkPos - 4).toInt
    val buf = new Array[Byte](len)
    data.seek(chunkPos)
    data.readFully(buf)
    val uncompressedData = Snappy.uncompress(buf)
    data.readInt()
    val is = new ByteArrayInputStream(uncompressedData)
    val dis = new DataInputStream(is)
    Chunk(offset, chunkPos, uncompressedData, is, dis)
  }

  private def consumeBytes[A](numBytes: Int)(f: DataInputStream => A) = {
    val buf = new Array[Byte](numBytes)
    if ( currentChunk.inputStream.available() < numBytes ) {
      val available = currentChunk.inputStream.available()
      currentChunk.inputStream.read(buf, 0, available)
      readNextChunk()
      currentChunk.inputStream.read(buf, available, numBytes-available)
    } else {
      currentChunk.inputStream.read(buf, 0, numBytes)
    }

    f(new DataInputStream(new ByteArrayInputStream(buf)))
  }

  def readBoolean() = consumeBytes(1) { _.readBoolean() }
  def readByte() = consumeBytes(1) { _.readByte() }
  def readChar() = consumeBytes(2) { _.readChar() }
  def readDouble() = consumeBytes(8) { _.readDouble() }
  def readFloat() = consumeBytes(4) { _.readFloat() }
  def readFully(buf: Array[Byte]) = consumeBytes(buf.length) { _.readFully(buf) }
  def readFully(buf: Array[Byte], offset: Int, length: Int) = consumeBytes(length) { _.readFully(buf, offset, length) }
  def readInt() = consumeBytes(4) { _.readInt() }
  def readLine() = { throw new Exception("Not implemented") }

  def readLong() = consumeBytes(8) { _.readLong() }
  def readShort() = consumeBytes(2) { _.readShort() }
  def readUnsignedByte() = consumeBytes(1) { _.readUnsignedByte() }
  def readUnsignedShort() = consumeBytes(2) { _.readUnsignedShort() }
  def readUTF() = { throw new Exception("not implemented") }
  def skipBytes(n: Int) = consumeBytes(n) { _ => n }
}