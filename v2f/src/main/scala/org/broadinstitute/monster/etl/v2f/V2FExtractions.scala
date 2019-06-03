package org.broadinstitute.monster.etl.v2f

import java.io.InputStreamReader
import java.nio.channels.Channels

import better.files._
import com.github.tototoshi.csv.{CSVFormat, CSVReader, TSVFormat}
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import io.circe.{Json, JsonObject}
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile

import scala.util.matching.Regex
import util.Try

/**
  * Ingest step converting and tranforming tsv's from v2f.
  *
  */
object V2FExtractions {

  /**
    * Given a root path that contains tsv(s) get the tsv as a ReadableFile
    *
    * @param tsvPath the root path containing tsv's to be converted
    * @param context the pipeline context for converting
    */
  def getReadableFile(
    tsvPath: String,
    context: ScioContext
  ): SCollection[FileIO.ReadableFile] =
    context.wrap {
      context.pipeline.apply(FileIO.`match`().filepattern(tsvPath))
    }.applyTransform[ReadableFile](FileIO.readMatches())

  /**
    * Given the ReadableFile that contains tsv(s) convert each tsv to json and get is filepath
    *
    * @param tableName the name of the tsv table that was converted to json
    */
  def tsvToJson(
    tableName: String
  ): SCollection[FileIO.ReadableFile] => SCollection[(JsonObject, String)] =
    _.transform(s"converting $tableName tsv(s) to json") { collection =>
      collection.flatMap { file =>
        Channels
          .newInputStream(file.open())
          .autoClosed
          .apply { path =>
            implicit val format: CSVFormat = new TSVFormat {}
            val reader = CSVReader.open(new InputStreamReader(path))
            reader.allWithHeaders().map { map =>
              val jsonObj = JsonObject.fromMap(map.map {
                case (key, value) =>
                  (camelCaseToSnakeCase(key), value)
              }.mapValues(Json.fromString))
              val filePath = file.getMetadata.resourceId.getCurrentDirectory.toString
              (jsonObj, filePath)
            }
          }
      }
    }

  /**
    * converts a string in camel case format to a string in snake case format using regex, replaces - with _'s as well
    *
    * @param string the string being converted
    */
  def camelCaseToSnakeCase(string: String): String = {
    string
      .replace(
        "-",
        "_"
      )
      .replaceAll(
        "([A-Z]+)([A-Z][a-z])",
        "$1_$2"
      )
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .toLowerCase
  }

  // the pattern of where the ancestryID is located in the tsv path
  // e.g: gs://path/to/metaanalysis/ancestry-specific/phenotype/ancestry=ancestryID/file
  val ancestryIDPattern: Regex = "\\/ancestry=([^\\/]+)\\/".r

  /**
    * adds the ancestry ID from the ReadableFile path as a field with a key, value of "ancestry" -> ancestryID to the json object
    *
    * @param tableName the name of the tsv table that was converted to json
    */
  def addAncestryID(
    tableName: String
  ): SCollection[(JsonObject, String)] => SCollection[(JsonObject, String)] = {
    _.transform(s"adding ancestry ID from $tableName's tsv path to its json") {
      collection =>
        collection.map {
          case (jsonObj, filePath) =>
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
            val jsonObjWithAddedField = addField(jsonObj, "ancestry", ancestryID)
            (jsonObjWithAddedField, filePath)
        }
    }
  }

  /**
    * adds a field to a json object, and overwrites the field if it already exists
    *
    * @param jsonObj the json that will have the fields to be added to it
    * @param fieldName the name of the field to be added to the json
    * @param jsonValue the json value of the field to be added to the json
    */
  def addField(jsonObj: JsonObject, fieldName: String, jsonValue: Json): JsonObject =
    jsonObj.add(fieldName, jsonValue)

  /**
    * Given a converstion function, for all the field names specified, the fields of a provided json object are converted
    *
    * @param tableName the name of the tsv table that was converted to json
    * @param fieldNames the names of fields to be converted
    * @param convertJsonString a function that converts a json string to another json type (i.e: number, boolean, array, etc..)
    */
  def convertJsonFieldsValueType(
    tableName: String,
    fieldNames: List[String],
    convertJsonString: (String, Json) => Json
  ): SCollection[(JsonObject, String)] => SCollection[(JsonObject, String)] =
    _.transform(s"enforing types to $tableName json values") { collection =>
      collection.map {
        case (jsonObj, filePath) =>
          fieldNames.foldLeft((jsonObj, filePath)) {
            case ((currentJsonObj, currentFilePath), fieldName) =>
              val jsonValue = currentJsonObj
                .apply(fieldName)
                .fold(
                  throw new Exception(
                    s"convertJsonFieldsValueType: error when calling apply on $fieldName"
                  )
                ) { json =>
                  convertJsonString(fieldName, json)
                }
              val transformedJson = addField(currentJsonObj, fieldName, jsonValue)
              (transformedJson, currentFilePath)
          }
      }
    }

  /**
    * converts a json string to a json double
    *
    * @param fieldName the field name of the json value being converted
    */
  def jsonStringToJsonDouble(fieldName: String, json: Json): Json =
    json.asString
      .fold(
        throw new Exception(
          s"jsonStringToJsonDouble: error when converting $fieldName value, $json, from type json string to type string"
        )
      ) {
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

  /**
    * converts a json string to a json int
    *
    * @param fieldName the field name of the json value being converted
    */
  def jsonStringToJsonInt(fieldName: String, json: Json): Json =
    json.asString
      .fold(
        throw new Exception(
          s"jsonStringToJsonInt: error when converting $fieldName value, $json, from type json string to type string"
        )
      ) {
        case "." => Json.fromString("nan")
        case ""  => Json.fromString("nan")
        case str =>
          Json.fromInt(
            Try(Integer.getInteger(str))
              .getOrElse(
                throw new Exception(
                  s"jsonStringToJsonInt: error when converting $fieldName value, $json, from type string to type int"
                )
              )
          )
      }

  /**
    * converts a json string to a json boolean
    *
    * @param fieldName the field name of the json value being converted
    */
  def jsonStringToJsonBoolean(fieldName: String, json: Json): Json =
    json.asString
      .fold(
        throw new Exception(
          s"jsonStringToJsonBoolean: error when converting $fieldName value, $json, from type json string to type string"
        )
      ) { str =>
        Json.fromBoolean(str == "1")
      }

  /**
    * converts a json string to a json array of json strings by splitting the json string with a delimeter
    *
    * @param delimeter the string (regex matching) that splits the json value/string into a json array
    * @param fieldName the field name of the json value being converted
    */
  def jsonStringToJsonArray(delimeter: String)(fieldName: String, json: Json): Json =
    json.asString
      .fold(
        throw new Exception(
          s"jsonStringToJsonArray: error when converting $fieldName value, $json, from type json string to type string"
        )
      ) { jsonStr =>
        Json.fromValues(jsonStr.split(delimeter).map { str =>
          Json.fromString(str)
        })
      }

  /**
    * converts a json array of json strings to a json array of json doubles
    *
    * @param fieldName the field name of the json value being converted
    */
  def convertJsonArrayStringToDouble(fieldName: String, json: Json): Json =
    json.asArray.fold(
      throw new Exception(
        s"convertJsonArrayStringToDouble: error when converting $fieldName value, $json, from a json array of type string to type Vector[Json]"
      )
    ) { jsonArray =>
      Json.fromValues(jsonArray.map { jsonStr =>
        jsonStringToJsonDouble(fieldName, jsonStr)
      })
    }
}
