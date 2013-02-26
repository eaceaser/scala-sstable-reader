package com.tehasdf.sstable.input

import com.tehasdf.sstable.CompressionInfoReader
import org.xerial.snappy.Snappy
import java.io.{ByteArrayInputStream, DataInputStream}
import com.tehasdf.sstable.CompressionInfo
import scala.annotation.tailrec

class SnappyCompressedSeekableDataStream(data: SeekableDataInputStream, compressionInfo: CompressionInfo) extends SeekableDataInputStream {
  case class Chunk(index: Int, startPosition: Long, data: Array[Byte], inputStream: ByteArrayInputStream, dataInputStream: DataInputStream)

  def length = compressionInfo.dataLength
  def position = currentPosition
  def seek(to: Long) = { throw new Exception("OMFG NOT IMPLEMENTED !!!") }
  
  def decompressEntireStream() = {
    val buf = new Array[Byte](length.toInt)
    recursivelyConsumeBytes(length.toInt, buf, 0)
    buf
  }
  
  private var currentOffset = 0
  private var currentPosition = 0L
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

  @tailrec
  private def recursivelyConsumeBytes(numBytes: Int, buf: Array[Byte], pos: Int) {
    if ( currentChunk.inputStream.available() < numBytes-pos ) {
      val available = currentChunk.inputStream.available()
      currentChunk.inputStream.read(buf, pos, available)
      if ((pos+available) < numBytes) {
        currentChunk = readNextChunk()
        recursivelyConsumeBytes(numBytes, buf, pos+available)
      }
    } else {
      currentChunk.inputStream.read(buf, pos, numBytes-pos)
    }
  }
  
  private def consumeBytes[A](numBytes: Int)(f: DataInputStream => A) = {
    val buf = new Array[Byte](numBytes)
    recursivelyConsumeBytes(numBytes, buf, 0)
    currentPosition += numBytes
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