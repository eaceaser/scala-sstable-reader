package com.tehasdf.sstable

trait CompressionInfo extends Iterator[Long] {
  def dataLength: Long
  def hasNext: Boolean
  def next(): Long
}

class SequenceBackedCompressionInfo(val dataLength: Long, offsets: Seq[Long]) extends CompressionInfo {
  private val it = offsets.iterator
  def hasNext = it.hasNext
  def next() = it.next()
}