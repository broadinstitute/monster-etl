package org.broadinstitute.monster.etl.v2f

import upack._

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

import scala.util.Try
import scala.util.matching.Regex

/**
  * Ingest utils converting and transforming TSVs from V2F.
  */
object V2FUtils {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]
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
    * Given the ReadableFile that contains TSVs convert each TSV to json and get is filepath.
    *
    * @param tableName the name of the TSV table that was converted to JSON
    */
  def tsvToJson(
    tableName: String
  ): SCollection[ReadableFile] => SCollection[(String, JsonObject)] =
    _.transform(s"Convert $tableName TSVs to JSON") { collection =>
      collection.flatMap { file =>
        Channels
          .newInputStream(file.open())
          .autoClosed
          .apply { path =>
            implicit val format: CSVFormat = new TSVFormat {}
            val reader = CSVReader.open(new InputStreamReader(path))
            reader.allWithHeaders().map { map =>
              val jsonObj = JsonObject.fromMap(map.collect {
                case (key, value) if value != "" =>
                  (camelCaseToSnakeCase(key), value.trim)
              }.mapValues(Json.fromString))
              val filePath = file.getMetadata.resourceId.getCurrentDirectory.toString
              (filePath, jsonObj)
            }
          }
      }
    }

  /**
    * Given the ReadableFile that contains TSVs convert each TSV to a Msg and get is filepath.
    *
    * @param tableName the name of the TSV table that was converted to Msg
    */
  def tsvToMsg(
    tableName: String
  ): SCollection[ReadableFile] => SCollection[(String, Msg)] =
    _.transform(s"Convert $tableName TSVs to Msg") { collection =>
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
                case (key, value) if value != "" =>
                  msgObj.value.update(Str(key), Str(value.trim))
              }
              val filePath = file.getMetadata.resourceId.getCurrentDirectory.toString
              (filePath, msgObj)
            }
          }
      }
    }

  /**
    * Converts a string in camel case format to a string in snake case format using regex, replaces - with _'s, and puts underscores before and after every number.
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
      .replaceAll("([a-z])([\\d])", "$1_$2")
      .replaceAll("([\\d])([a-z])", "$1_$2")
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
    fieldNames: List[String],
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
    * Converts a JSON String to a JSON Boolean.
    *
    * @param fieldName the field name of the json value being converted
    * @param json the json value being converted
    */
  def jsonStringToJsonBoolean(fieldName: String, json: Json): Json = {
    val jsonStr = jsonStringToString(fieldName, json)
    Json.fromBoolean(jsonStr == "1" || jsonStr == "true")
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
    * Renames JSON fields in a collection of JSON Objects.
    *
    * @param tableName the name of the TSV table that was converted to JSON
    * @param fieldsToRename the map of Json Fields to be renamed (old, new)
    */
  def renameFields(
    tableName: String,
    fieldsToRename: Map[String, String]
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] = {
    _.transform(s"Renaming fields in $tableName JSONs") { collection =>
      collection.map {
        case (filePath, jsonObj) =>
          fieldsToRename.foldLeft((filePath, jsonObj)) {
            case ((currentFilePath, currentJsonObj), (oldFieldName, newFieldName)) =>
              val renamedJsonObj = currentJsonObj
                .apply(oldFieldName)
                .fold(currentJsonObj) { jsonValue =>
                  currentJsonObj.add(newFieldName, jsonValue).remove(oldFieldName)
                }
              (currentFilePath, renamedJsonObj)
          }
      }
    }
  }

  /**
    * Extracts variant JSON fields from a collection of JSON Objects and outputs a new JSON Object with those fields.
    *
    * @param tableName the name of the TSV table that was converted to JSON
    * @param variantFieldNames the lists of Json Fields names to be extracted
    */
  def extractVariantFields(
    tableName: String,
    variantFieldNames: List[String]
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] = {
    _.transform(s"Extracting variant fields from $tableName") { collection =>
      collection.map {
        case (filePath, jsonObj) =>
          filePath -> JsonObject.fromMap(
            variantFieldNames
              .foldLeft(Map.empty[String, String]) {
                case (currentMap, currentVariantFieldName) =>
                  val currentVariantFieldValue = jsonObj
                    .apply(currentVariantFieldName)
                    .fold(
                      throw new Exception(
                        s"extractVariantFields: error when getting variant $currentVariantFieldName from $tableName's JSON"
                      )
                    ) { jsonValue =>
                      jsonStringToString(currentVariantFieldName, jsonValue)
                    }
                  val updatedMap =
                    currentMap.updated(currentVariantFieldName, currentVariantFieldValue)
                  val variantId = updatedMap.get("id").fold(currentVariantFieldValue) {
                    currentVariantId =>
                      s"$currentVariantId:$currentVariantFieldValue"
                  }
                  updatedMap.updated("id", variantId)
              }
              .mapValues(Json.fromString)
          )
      }
    }
  }

  /**
    * Removes JSON fields from a collection of JSON Objects.
    *
    * @param tableName the name of the TSV table that was converted to JSON
    * @param fieldNamesToRemove the lists of Json Fields to be removed
    */
  def removeFields(
    tableName: String,
    fieldNamesToRemove: List[String]
  ): SCollection[(String, JsonObject)] => SCollection[(String, JsonObject)] = {
    _.transform(s"Removing fields from $tableName") { collection =>
      collection.map {
        case (filePath, jsonObj) =>
          filePath -> fieldNamesToRemove.foldLeft(jsonObj) {
            case (currentJsonObj, currentVariantFieldName) =>
              currentJsonObj.remove(currentVariantFieldName)
          }
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
