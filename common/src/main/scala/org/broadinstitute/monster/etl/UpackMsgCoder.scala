package org.broadinstitute.monster.etl

import java.io.{InputStream, OutputStream}

import org.apache.beam.repackaged.sql.com.google.common.io.ByteStreams
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.util.VarInt
import upack.Msg

/** Serde for upack messages. */
class UpackMsgCoder extends Coder[Msg] {

  override def encode(value: Msg, outStream: OutputStream): Unit =
    Option(value).foreach { msg =>
      val bytes = upack.write(msg)
      VarInt.encode(bytes.length, outStream)
      outStream.write(bytes)
    }

  override def decode(inStream: InputStream): Msg = {
    val numBytes = VarInt.decodeInt(inStream)
    val bytes = new Array[Byte](numBytes)
    ByteStreams.readFully(inStream, bytes)
    upack.read(bytes)
  }

  override def getCoderArguments: java.util.List[_ <: Coder[_]] =
    java.util.Collections.emptyList()

  override def verifyDeterministic(): Unit =
    throw new Coder.NonDeterministicException(
      this,
      "Message encoding doesn't enforce key order"
    )
}
