package com.tehasdf.sstable

import org.scalatest.FunSuite
import org.xerial.snappy.SnappyInputStream
import java.nio.ByteBuffer
import java.io.FileInputStream
import java.io.File
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.DataInputStream
import com.tehasdf.sstable.input.FileSeekableDataInputStream
import com.tehasdf.sstable.input.SnappyCompressedSeekableDataStream

@RunWith(classOf[JUnitRunner])
class BasicTest extends FunSuite {
  test("basic spec") {
/*    val len = tableIs.readInt()
    println("SEG: " + len)

/*    tableIs.readByte()
    tableIs.readByte()
    tableIs.readByte()
    tableIs.readByte() */

    val b1 = (tableIs.readByte() & 0xFF) << 8
    val length = b1 | (tableIs.readByte() & 0xFF)

    println(b1)
    println(length)

    println("LENGTH: %d".format(length))

    val buff = new Array[Byte](length)
    tableIs.readFully(buff)

    val keyBuf = ByteBuffer.wrap(buff)

    println("wtf: %s".format(new String(keyBuf.array(), "UTF-8")))

    val dataLength = tableIs.readInt()
    println(dataLength)

    val colData = new Array[Byte](dataLength)
    tableIs.readFully(colData)

    val b2 = (tableIs.readByte() & 0xFF) << 8
    val length2 = b2 | (tableIs.readByte() & 0xFF)

    println(length2) */

    val ci = new File("/Users/eac/stupid/blobstore-StorageNodeMetadata-hc-1126711-CompressionInfo.db")
    val data = new File("/Users/eac/stupid/blobstore-StorageNodeMetadata-hc-1126711-Data.db")
    val index = new File("/Users/eac/stupid/blobstore-StorageNodeMetadata-hc-1126711-Index.db")
    val reader = new CompressedDataReader(new SnappyCompressedSeekableDataStream(new FileSeekableDataInputStream(data), new CompressionInfoReader(new FileInputStream(ci))))
    reader.foreach { data =>
      println(data)
    }
  }
}
