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
import util.Try

/**
  * Ingest step converting and tranforming tsv's from v2f.
  *
  */
object V2FExtractions {

  /**
    * Given a root path that contains tsv(s) convert each tsv to json
    *
    * @param tsvPath the root path containing tsv's to be converted
    * @param context the pipeline context for converting
    */
  def tsvToJson(tsvPath: String, context: ScioContext): SCollection[JsonObject] =
    context.wrap {
      context.pipeline.apply(FileIO.`match`().filepattern(tsvPath))
    }.applyTransform[ReadableFile](FileIO.readMatches()).flatMap { file =>
      Channels
        .newInputStream(file.open())
        .autoClosed
        .apply { path =>
          implicit val format: CSVFormat = new TSVFormat {}
          val reader = CSVReader.open(new InputStreamReader(path))
          reader.allWithHeaders().map { map =>
            JsonObject.fromMap(map.map {
              case (key, value) =>
                (camelCaseToSnakeCase(key), value)
            }.mapValues(Json.fromString))
          }
        }
    }

  /**
    * converts a string in camel case format to a string in snake case format using regex
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

  /**
    * adds given field(s) to a json object
    *
    * @param jsonObj the json that will have the fields to be added to it
    * @param fields the fields to be added where the key is the name and the value is the json
    */
  def addFields(jsonObj: JsonObject, fields: Map[String, Json]): JsonObject =
    fields.foldLeft(jsonObj) {
      case (currentJsonObj, currentField) =>
        addField(currentJsonObj, currentField._1, currentField._2)
    }

  /**
    * renames given field(s) of a json object
    *
    * @param jsonObj the json that will have the fields renamed
    * @param fieldNamesToChange the fields to be added where the key is the old name and the value is the new name
    */
  def renameFields(
    jsonObj: JsonObject,
    fieldNamesToChange: Map[String, String]
  ): JsonObject =
    fieldNamesToChange.foldLeft(jsonObj) {
      case (currentJsonObj, fieldNameToChange) =>
        currentJsonObj
          .apply(fieldNameToChange._1)
          .fold(
            throw new Exception(
              s"renameFields: error when calling apply on ${fieldNameToChange._1}"
            )
          ) { jsonValue =>
            addField(currentJsonObj, fieldNameToChange._2, jsonValue)
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
  ): SCollection[JsonObject] => SCollection[JsonObject] =
    _.transform(s"enforing types to $tableName json values") { collection =>
      collection.map { jsonObj =>
        fieldNames.foldLeft(jsonObj) {
          case (currentJsonObj, fieldName) =>
            val jsonValue = currentJsonObj
              .apply(fieldName)
              .fold(
                throw new Exception(
                  s"convertJsonFieldsValueType: error when calling apply on $fieldName"
                )
              ) { json =>
                convertJsonString(fieldName, json)
              }
            addField(currentJsonObj, fieldName, jsonValue)
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
            Try((Integer.getInteger(str)))
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
