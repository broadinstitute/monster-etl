package org.broadinstitute.monster.etl.v2f

import java.io.InputStreamReader
import java.nio.channels.Channels

import better.files._
import com.github.tototoshi.csv.{CSVFormat, CSVReader, TSVFormat}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import org.apache.beam.sdk.io.{FileIO, ReadableFileCoder}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.broadinstitute.monster.etl.UpackMsgCoder
import upack._

import scala.util.matching.Regex

/**
  * Ingest utils converting and transforming TSVs from V2F.
  */
object V2FUtils {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.beam(new ReadableFileCoder)

  /**
    * Given a pattern matching TSVs, get the TSVs as ReadableFiles.
    *
    * @param tsvPath the root path containing TSVs to be converted
    * @param context context of the main V2F pipeline
    */
  def getReadableFiles(
    tsvPath: String,
    context: ScioContext
  ): SCollection[FileIO.ReadableFile] =
    context.wrap {
      context.pipeline.apply(
        FileIO
          .`match`()
          .filepattern(tsvPath)
          .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)
      )
    }.applyTransform[ReadableFile](FileIO.readMatches())

  /**
    * Given the SCollection of ReadableFiles that contains TSVs convert each TSV to a Msg and get its filepath.
    *
    * @param tableName the name of the TSV table that was converted to Msg
    */
  def tsvToMsg(
    tableName: String
  ): SCollection[ReadableFile] => SCollection[(String, Msg)] =
    _.transform(s"Extract $tableName TSV rows") { collection =>
      collection.flatMap { file =>
        Channels
          .newInputStream(file.open())
          .autoClosed
          .apply { path =>
            implicit val format: CSVFormat = new TSVFormat {}
            val reader = CSVReader.open(new InputStreamReader(path))
            reader.allWithHeaders().map { map =>
              val msgObj = Obj()
              map.foreach {
                case (key, value) =>
                  val trimmed = value.trim
                  if (trimmed != "") {
                    msgObj.value.update(Str(key), Str(trimmed))
                  }
              }
              val filePath = file.getMetadata.resourceId.getCurrentDirectory.toString
              (filePath, msgObj)
            }
          }
      }
    }

  // the pattern of where the ancestryID is located in the tsv path
  // e.g: gs://path/to/metaanalysis/ancestry-specific/phenotype/ancestry=ancestryID/file
  val ancestryIDPattern: Regex = "\\/ancestry=([^\\/]+)\\/".r

  /**
    * Adds the ancestry ID from the ReadableFile paths as a field with a key, value of "ancestry" -> ancestryID to the Msg Objects.
    *
    * @param tableName the name of the tsv table that was converted to Msg
    */
  def addAncestryID(
    tableName: String
  ): SCollection[(String, Msg)] => SCollection[(String, Msg)] = {
    _.transform(s"Adding the ancestry ID from $tableName's tsv path to its Msg object") {
      collection =>
        collection.map {
          case (filePath, msgObj) =>
            val toRet = upack.copy(msgObj)
            val underlying = toRet.obj
            val ancestryID = Str(
              ancestryIDPattern
                .findFirstMatchIn(filePath)
                .getOrElse(
                  throw new Exception(
                    s"addAncestryID: error when finding ancestry ID from $tableName tsv path, ($filePath), using $ancestryIDPattern as a pattern"
                  )
                )
                .group(1)
            )
            underlying.update(Str("ancestry"), ancestryID)
            (filePath, toRet)
        }
    }
  }

}
