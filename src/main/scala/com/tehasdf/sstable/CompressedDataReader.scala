package com.tehasdf.sstable

import java.io.File
import java.io.DataInputStream
import java.io.FileInputStream
import scala.collection.JavaConversions._
import org.xerial.snappy.Snappy

class CompressedDataReader(dataFile: File, indexFile: File, metadataFile: File) extends Iterator[Array[Byte]] {
  var chunkLength: Int = 9
  
  private def readOffsets() = {
    val is = new DataInputStream(new FileInputStream(metadataFile))
    
    val compressor = is.readUTF()
    val numOpts = is.readInt()
    
    for (i <- 0 until numOpts) {
      val k = is.readUTF()
      val v = is.readUTF()
    }
    
    chunkLength = is.readInt()
    val dataLength = is.readLong()
    val chunkCount = is.readInt()
    
    val offsets = new java.util.ArrayList[Long]
    for (i <- 0 until chunkCount) {
      val size = is.readLong()
      offsets.add(size)
    }
    
    offsets.toList
  }
  
  private val offsets = readOffsets()
  private val index = new IndexReader(indexFile)
  
  private val fileIs = new FileInputStream(dataFile)
  private val chan = fileIs.getChannel()
  private val is = new DataInputStream(fileIs)
  
  def hasNext = index.hasNext
  
  def next() = {
    val key = index.next
    val chunkIndex: Int = (key.pos / chunkLength).toInt
    val chunkPos = offsets(chunkIndex)
      
    val nextChunkPos = offsets.lift(chunkIndex+1).getOrElse(dataFile.length())
    val len = (nextChunkPos - chunkPos - 4).toInt
      
    val buf = new Array[Byte](len)
    chan.position(chunkPos)
    is.readFully(buf)
    val uncompressed = Snappy.uncompress(buf)
    println(uncompressed)
    is.readInt()
    
    uncompressed
  }
}