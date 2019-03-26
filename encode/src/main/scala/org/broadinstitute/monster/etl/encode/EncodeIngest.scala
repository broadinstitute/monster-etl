package org.broadinstitute.monster.etl.encode

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import caseapp._
import com.spotify.scio.coders.Coder
import com.spotify.scio.{BuildInfo => _, io => _, _}
import com.spotify.scio.extra.json._
import io.circe.JsonObject
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.beam.sdk
import org.apache.beam.sdk.coders.{CoderException, StructuredCoder}
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.ByteStreams
import org.broadinstitute.monster.etl.BuildInfo

/** Main entry-point for the ENCODE ETL workflow. */
object EncodeIngest {

  @AppName("ENCODE Ingest")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.encode.EncodeIngest")
  case class Args(
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE experiments")
    experimentsJson: String,
    @HelpMessage("Path to directory where ETL output JSON should be written")
    outputDir: String
  )

  /**
    * Converter to/from circe's JSON representation into/out of Beam bytes.
    */
  implicit def circeCoder[A: Encoder: Decoder]: Coder[A] = Coder.beam {
    new StructuredCoder[A] {
      private val parser = new JawnParser()

      override def encode(value: A, outStream: OutputStream): Unit =
        Option(value).foreach { v =>
          val bytes = v.asJson.noSpaces.getBytes
          VarInt.encode(bytes.length, outStream)
          outStream.write(bytes)
        }

      override def decode(inStream: InputStream): A = {
        val numBytes = VarInt.decodeInt(inStream)
        val bytes = new Array[Byte](numBytes)
        ByteStreams.readFully(inStream, bytes)
        parser
          .decodeByteBuffer[A](ByteBuffer.wrap(bytes))
          .fold(err => throw new CoderException("Could not decode JSON", err), identity)
      }

      override def getCoderArguments: java.util.List[_ <: sdk.coders.Coder[_]] =
        java.util.Collections.emptyList()

      override def verifyDeterministic(): Unit = ()
    }
  }

  def main(rawArgs: Array[String]): Unit = {
    // Using `typed` gives us '--help' and '--usage' automatically.
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // Add processing steps between the read and write here.
    pipelineContext
      .jsonFile[JsonObject](parsedArgs.experimentsJson)
      .saveAsJsonFile(parsedArgs.outputDir)

    pipelineContext.close().waitUntilDone()
    ()
  }
}
