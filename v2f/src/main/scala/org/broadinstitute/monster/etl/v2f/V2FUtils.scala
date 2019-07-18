package org.broadinstitute.monster.etl.v2f

import java.io.InputStreamReader
import java.nio.channels.Channels

import better.files._
import com.github.tototoshi.csv.{CSVFormat, CSVReader, TSVFormat}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe.{Json, JsonObject}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.broadinstitute.monster.etl.{MsgTransformations, UpackMsgCoder}
import upack._

import scala.util.Try
import scala.util.matching.Regex

/**
  * Ingest utils converting and transforming TSVs from V2F.
  */
object V2FUtils {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)
  implicit val readableFileCoder: Coder[ReadableFile] = Coder.kryo[ReadableFile]

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
      context.pipeline.apply(FileIO.`match`().filepattern(tsvPath))
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
    * Adds the ancestry ID from the ReadableFile paths as a field with a key, value of "ancestry" -> ancestryID to the JSON Objects.
    *
    * @param tableName the name of the tsv table that was converted to json
    */
  def addAncestryID(
    tableName: String
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] = {
    _.transform(s"Adding the ancestry ID from $tableName's tsv path to its json") {
      collection =>
        collection.map {
          case (filePath, jsonObj) =>
            val ancestryID = Json.fromString(
              ancestryIDPattern
                .findFirstMatchIn(filePath)
                .getOrElse(
                  throw new Exception(
                    s"addAncestryID: error when finding ancestry ID from $tableName tsv path, ($filePath), using $ancestryIDPattern as a pattern"
                  )
                )
                .group(1)
            )
            val jsonObjWithAddedField = jsonObj.add("ancestry", ancestryID)
            (filePath, jsonObjWithAddedField)
        }
    }
  }

  /**
    * Given a conversion function, for all the field names specified, the fields of a provided JSON Object are converted based on that function.
    *
    * @param tableName the name of the tsv table that was converted to json
    * @param fieldNames the names of fields to be converted
    * @param convertJsonString a function that converts a json string to another json type (i.e: number, boolean, array, etc..)
    */
  def convertJsonFieldsValueType(
    tableName: String,
    fieldNames: Set[String],
    convertJsonString: (String, Json) => Json
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] =
    _.transform(s"Convert $tableName fields to expected types") { collection =>
      collection.map {
        case (filePath, jsonObj) =>
          filePath -> fieldNames.foldLeft((jsonObj)) {
            case (currentJsonObj, fieldName) =>
              currentJsonObj
                .apply(fieldName)
                .fold(
                  currentJsonObj
                ) { json =>
                  val transformedJson =
                    currentJsonObj.add(fieldName, convertJsonString(fieldName, json))
                  transformedJson
                }
          }
      }
    }

  /**
    * Converts a JSON String to a JSON Double.
    *
    * @param fieldName the field name of the json value being converted
    * @param json the json value being converted
    */
  def jsonStringToJsonDouble(fieldName: String, json: Json): Json = {
    val jsonStr = jsonStringToString(fieldName, json)
    jsonStr match {
      case "." => Json.fromString("nan")
      case ""  => Json.fromString("nan")
      case str =>
        Json.fromDoubleOrString(
          Try(str.toDouble)
            .getOrElse(
              throw new Exception(
                s"jsonStringToJsonDouble: error when converting $fieldName value, $json, from type string to type double"
              )
            )
        )
    }
  }

  /**
    * Converts a JSON String to a JSON Long.
    *
    * @param fieldName the field name of the json value being converted
    * @param json the json value being converted
    */
  def jsonStringToJsonLong(fieldName: String, json: Json): Json = {
    val jsonStr = jsonStringToString(fieldName, json)
    jsonStr match {
      case "." => Json.fromString("nan")
      case ""  => Json.fromString("nan")
      case str =>
        val strippedStr =
          if (str.endsWith(".0")) {
            str.dropRight(2)
          } else {
            str
          }
        Json.fromLong(
          Try(strippedStr.toLong)
            .getOrElse(
              throw new Exception(
                s"jsonStringToJsonLong: error when converting $fieldName: $json from type string to type Long"
              )
            )
        )
    }
  }

  /**
    * Converts a JSON String to a JSON Array of Strings by splitting the JSON String with a delimiter.
    *
    * @param delimeter the string (regex matching) that splits the json value/string into a json array
    * @param fieldName the field name of the json value being converted
    */
  def jsonStringToJsonArray(delimeter: String)(fieldName: String, json: Json): Json = {
    val jsonStr = jsonStringToString(fieldName, json)
    Json.fromValues(jsonStr.split(delimeter).map { str =>
      Json.fromString(str.trim)
    })
  }

  /**
    * Converts a JSON Array of Strings to a JSON Array of Doubles.
    *
    * @param fieldName the field name of the json value being converted
    * @param json the json value being converted
    */
  def convertJsonArrayStringToDouble(fieldName: String, json: Json): Json =
    json.asArray.fold(
      throw new Exception(
        s"convertJsonArrayStringToDouble: error when converting $fieldName: $json from a json array of type string to type Vector[Json]"
      )
    ) { jsonArray =>
      Json.fromValues(jsonArray.map { jsonStr =>
        jsonStringToJsonDouble(fieldName, jsonStr)
      })
    }

  /**
    * Extracts variant Msg fields from a collection of Msg Objects and outputs a new Msg Object with those fields.
    *
    * @param tableName the name of the TSV table that was converted to Msg
    * @param variantFieldNames the lists of Msg Fields names to be extracted
    */
  def extractVariantFields(
    tableName: String,
    variantFieldNames: Set[String]
  ): SCollection[(String, Msg)] => SCollection[(String, Msg)] = {
    _.transform(s"Extracting variant fields from $tableName") { collection =>
      collection.map {
        case (filePath, msgObj) =>
          // extract fields
          val withExtractedFields =
            MsgTransformations.extractFields(variantFieldNames)(msgObj)
          (filePath, withExtractedFields)
      }
    }
  }

  /**
    * Converts JSON fields values from type JSON String to type String.
    *
    * @param fieldName the field name of the json value being converted
    * @param json the json value being converted
    */
  def jsonStringToString(fieldName: String, json: Json): String =
    json.asString
      .getOrElse(
        throw new Exception(
          s"jsonStringToJsonBoolean: error when converting $fieldName: $json from type json string to type string"
        )
      )
}
