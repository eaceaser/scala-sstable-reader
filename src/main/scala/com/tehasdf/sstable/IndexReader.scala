package com.tehasdf.sstable

import com.tehasdf.sstable.input.{FileSeekableDataInputStream, SeekableDataInputStream}

import java.io.{File, IOException}

import scala.collection.Iterator

case class Key(name: String, pos: Long)

class IndexReader(index: SeekableDataInputStream) extends Iterator[Key] {
  def this(indexFile: File) = this(new FileSeekableDataInputStream(indexFile))

  def hasNext = index.position < index.length

  private def remaining = index.length - index.position

  def next() = {
    if (remaining < 2) {
      index.seek(index.length)
      throw new IOException("Not enough bytes remaining to read key length")
    } else {
      val tempByte = (index.readByte() & 0xFF) << 8
      val keyLen = tempByte | (index.readByte & 0xFF)
      if (remaining < keyLen) {
        index.seek(index.length)
        throw new IOException("Not enough bytes rmaining to read key")
      } else {
        val buf = new Array[Byte](keyLen)
        index.readFully(buf)
        val key = new String(buf)
        val pos = index.readLong()
        val rv = Key(key, pos)
        rv
      }
    }
  }
}