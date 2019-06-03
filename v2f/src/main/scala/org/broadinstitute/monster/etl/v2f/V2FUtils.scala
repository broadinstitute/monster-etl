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
object V2FUtils {

  /**
    * Given a pattern matching TSVs, get the TSVs as ReadableFiles.
    *
    * @param tsvPath the root path containing TSV's to be converted
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
    * Given the ReadableFile that contains TSVs convert each TSV to json and get is filepath.
    *
    * @param tableName the name of the TSV table that was converted to JSON
    */
  def tsvToJson(
    tableName: String
  ): SCollection[FileIO.ReadableFile] => SCollection[(String, JsonObject)] =
    _.transform(s"Convert $tableName TSVs to JSON") { collection =>
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
              (filePath, jsonObj)
            }
          }
      }
    }

  /**
    * Converts a string in camel case format to a string in snake case format using regex, replaces - with _'s as well.
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
            val jsonObjWithAddedField = addField(jsonObj, "ancestry", ancestryID)
            (filePath, jsonObjWithAddedField)
        }
    }
  }

  /**
    * Adds a field to a JSON Object, and overwrites the field if it already exists.
    *
    * @param jsonObj the json that will have the fields to be added to it
    * @param fieldName the name of the field to be added to the json
    * @param jsonValue the json value of the field to be added to the json
    */
  def addField(jsonObj: JsonObject, fieldName: String, jsonValue: Json): JsonObject =
    jsonObj.add(fieldName, jsonValue)

  /**
    * Given a conversion function, for all the field names specified, the fields of a provided JSON Object are converted based on that function.
    *
    * @param tableName the name of the tsv table that was converted to json
    * @param fieldNames the names of fields to be converted
    * @param convertJsonString a function that converts a json string to another json type (i.e: number, boolean, array, etc..)
    */
  def convertJsonFieldsValueType(
    tableName: String,
    fieldNames: List[String],
    convertJsonString: (String, Json) => Json
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] =
    _.transform(s"Convert $tableName fields to expected types") { collection =>
      collection.map {
        case (filePath, jsonObj) =>
          fieldNames.foldLeft((filePath, jsonObj)) {
            case ((currentFilePath, currentJsonObj), fieldName) =>
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
              (currentFilePath, transformedJson)
          }
      }
    }

  /**
    * Converts a JSON String to a JSON Double.
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
    * Converts a JSON String to a JSON Integer.
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
    * Converts a JSON String to a JSON Boolean.
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
        Json.fromBoolean(str == "1" || str == "true")
      }

  /**
    * Converts a JSON String to a JSON Array of Strings by splitting the JSON String with a delimiter.
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
    * Converts a JSON Array of Strings to a JSON Array of Doubles.
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
