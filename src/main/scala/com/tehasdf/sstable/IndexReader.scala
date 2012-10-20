package com.tehasdf.sstable

import java.io.File
import scala.collection.Iterator
import java.io.FileInputStream
import java.io.DataInputStream
import java.io.InputStream
import com.tehasdf.sstable.input.SeekableDataInputStream
import com.tehasdf.sstable.input.FileSeekableDataInputStream

case class Key(name: String, pos: Long)

class IndexReader(index: SeekableDataInputStream) extends Iterator[Key] {
  def this(indexFile: File) = this(new FileSeekableDataInputStream(indexFile))
  def hasNext = index.position < index.length
  def next() = {
    val tempByte = (index.readByte() & 0xFF) << 8
    val keyLen = tempByte | (index.readByte & 0xFF)
    val buf = new Array[Byte](keyLen)
    index.readFully(buf)
    val key = new String(buf)
    val pos = index.readLong()
    Key(key, pos)
  }
}