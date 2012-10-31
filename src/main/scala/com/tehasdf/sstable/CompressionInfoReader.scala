package com.tehasdf.sstable

import java.io.{DataInputStream, InputStream}

class CompressionInfoReader(is: InputStream) extends Iterator[Long] {
  val dis = new DataInputStream(is)
  val compressor = dis.readUTF()
  val numOpts = dis.readInt()
  
  for (i <- 0 until numOpts) {
    dis.readUTF()
    dis.readUTF()
  }
  
  val chunkLength = dis.readInt()
  val dataLength = dis.readLong()
  val chunkCount = dis.readInt()
  
  var currentChunk = 0
  
  def hasNext = currentChunk < chunkCount
  def next() = {
    currentChunk += 1
    dis.readLong()
  }
}