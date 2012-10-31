package com.tehasdf.sstable.input

import org.xerial.snappy.Snappy
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}
import java.io.File
import java.io.FileOutputStream

object InMemorySeekableDataStream {
  def fromSnappyCompressedData(compressedData: Array[Byte], chunkOffsets: Seq[Long]) = {
    val compressedIs = new DataInputStream(new ByteArrayInputStream(compressedData))
    val baos = new ByteArrayOutputStream
    chunkOffsets.zipWithIndex.foreach { case (cur, idx) =>
      val next = chunkOffsets.lift(idx+1).getOrElse(compressedData.length.toLong)
      val buf = new Array[Byte]((next-cur-4).toInt)
      compressedIs.readFully(buf)
      compressedIs.skipBytes(4)
      val uncompressed = Snappy.uncompress(buf)
      baos.write(uncompressed)
    }

    val uncompressedData = baos.toByteArray()
    new InMemorySeekableDataStream(uncompressedData)
  }
}
class InMemorySeekableDataStream(buf: Array[Byte], is: ByteArrayInputStream) extends SeekableDataInputStreamProxy(new DataInputStream(is)) {
  def this(buf: Array[Byte]) = this(buf, new ByteArrayInputStream(buf))
  def length = buf.length
  def position = length - is.available()
  def seek(to: Long) { is.reset(); is.skip(to) }
}