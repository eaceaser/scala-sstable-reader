package com.tehasdf.sstable

import input.{FileSeekableDataInputStream, SeekableDataInputStream}
import java.io.File

case class IndexPosition(key: String, location: Long)

class IndexSummaryReader(summary: SeekableDataInputStream) extends Iterator[IndexPosition] {
  def this(indexSummaryFile: File) = this(new FileSeekableDataInputStream(indexSummaryFile))

  val indexInterval = summary.readInt()
  private val totalEntries = summary.readInt()
  private var pos = 0

  def hasNext = pos < totalEntries
  def next() = {
    val location = summary.readLong()
    val keyLen = summary.readInt()
    val buf = new Array[Byte](keyLen)
    summary.readFully(buf)
    val key = new String(buf)

    pos += 1
    IndexPosition(key, location)
  }
}