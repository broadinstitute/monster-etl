package org.broadinstitute.monster.etl.encode

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import caseapp._
import com.spotify.scio.coders.Coder
import com.spotify.scio.{BuildInfo => _, io => _, _}
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
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

  private val rawExperimentFields = Set(
    "@id",
    "accession",
    "aliases",
    "award",
    "date_created",
    "date_released",
    "date_submitted",
    "dbxrefs",
    "description",
    "lab",
    "status",
    "submitted_by",
    "target"
  )

  private val experimentFieldsToRename = Map(
    "@id" -> "label",
    "accession" -> "close_match",
    "award" -> "sponsor",
    "date_created" -> "created_at"
  )

  private val experimentLinkFields =
    Set("close_match", "sponsor", "submitted_by", "target")

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

      override def verifyDeterministic(): Unit = ()
    }
  }

  def main(rawArgs: Array[String]): Unit = {
    // Using `typed` gives us '--help' and '--usage' automatically.
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    val rawExperiments = pipelineContext.jsonFile[JsonObject](parsedArgs.experimentsJson)

    val cleanedExperiments = rawExperiments
      .transform("Trim Experiment Fields")(trimFields(rawExperimentFields))
      .transform("Rename Experiment Fields")(renameFields(experimentFieldsToRename))
      .transform("Build Experiment Links")(buildLinks(experimentLinkFields))
      .transform("Extract Experiment Labels")(extractLabels)
      .transform("Combine Experiment Aliases")(_.map { json =>
        val allAliases = Vector("aliases", "dbxrefs").flatMap { field =>
          json(field).flatMap(_.asArray).getOrElse(Vector.empty)
        }
        json.add("aliases", allAliases.asJson).remove("dbxrefs")
      })

    cleanedExperiments.saveAsJsonFile(s"${parsedArgs.outputDir}/experiments")

    pipelineContext.close().waitUntilDone()
    ()
  }

  type JsonPipe = SCollection[JsonObject] => SCollection[JsonObject]

  private val extractLabels: JsonPipe = { stream =>
    val idRegex = "/[^/]+/(.+)/".r

    stream.map { json =>
      val extracted = for {
        idJson <- json("label")
        idString <- idJson.asString
        label <- idRegex.findFirstMatchIn(idString)
      } yield {
        label
      }

      extracted
        .fold(json) { labelMatch =>
          val label = labelMatch.group(1)
          json
            .add("label", label.asJson)
            .add("id", s"Broad-$label".asJson)
        }
    }
  }

  private def trimFields(fieldsToKeep: Set[String]): JsonPipe =
    _.map(json => json.filterKeys(fieldsToKeep))

  private def renameFields(fieldsToRename: Map[String, String]): JsonPipe =
    _.map { json =>
      fieldsToRename.foldLeft(json) {
        case (renamedSoFar, (oldName, newName)) =>
          renamedSoFar(oldName).fold(renamedSoFar) { value =>
            renamedSoFar.add(newName, value).remove(oldName)
          }
      }
    }

  private def buildLinks(linkFields: Set[String]): JsonPipe =
    _.map { json =>
      linkFields.foldLeft(json) { (linkedSoFar, fieldName) =>
        val linkValue = for {
          valueJson <- linkedSoFar(fieldName)
          valueString <- valueJson.asString
        } yield {
          if (valueString.charAt(0) == '/') {
            s"http://www.encodeproject.org$valueString"
          } else {
            s"http://www.encodeproject.org/$valueString"
          }
        }

        linkValue.fold(linkedSoFar.remove(fieldName)) { link =>
          linkedSoFar.add(fieldName, link.asJson)
        }
      }
    }
}
