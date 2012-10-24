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
import java.util.UUID

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
    tableIs.readFully(colData) */
  }
}
