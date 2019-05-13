package org.broadinstitute.monster.etl

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.beam.sdk
import org.apache.beam.sdk.coders.{CoderException, StructuredCoder, Coder => JCoder}
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.ByteStreams

package object encode {

  /** Convenience alias for transformations that operate on JSON. */
  type JsonPipe = SCollection[JsonObject] => SCollection[JsonObject]

  /**
    * Converter to/from circe's JSON representation into/out of Beam bytes.
    */
  implicit val jsonObjectCoder: Coder[JsonObject] = Coder.beam {
    new StructuredCoder[JsonObject] {
      private val parser = new JawnParser()

      override def encode(value: JsonObject, outStream: OutputStream): Unit =
        Option(value).foreach { v =>
          val bytes = v.asJson.noSpaces.getBytes
          VarInt.encode(bytes.length, outStream)
          outStream.write(bytes)
        }

      override def decode(inStream: InputStream): JsonObject = {
        val numBytes = VarInt.decodeInt(inStream)
        val bytes = new Array[Byte](numBytes)
        ByteStreams.readFully(inStream, bytes)
        parser
          .decodeByteBuffer[JsonObject](ByteBuffer.wrap(bytes))
          .fold(err => throw new CoderException("Could not decode JSON", err), identity)
      }

      override def getCoderArguments: java.util.List[_ <: sdk.coders.Coder[_]] =
        java.util.Collections.emptyList()

      override def verifyDeterministic(): Unit =
        throw new JCoder.NonDeterministicException(
          this,
          "JSON object encoding doesn't preserve key order"
        )
    }
  }
}
