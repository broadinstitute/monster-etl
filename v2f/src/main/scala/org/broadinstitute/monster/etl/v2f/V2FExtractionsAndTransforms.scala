package org.broadinstitute.monster.etl.v2f

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.json._
import io.circe.JsonObject

object V2FExtractionsAndTransforms {

  /**
    * Given a pattern matching TSVs, get the TSVs as ReadableFiles and convert each TSV to json and get is filepath.
    *
    * @param v2fConstant the type of tsv(s) that will be extracted and converted to Json
    * @param context context of the main V2F pipeline
    * @param inputDir the root directory containing TSV's to be converted
    * @param relativeFilePath the file path containing TSV's to be converted relative to the input root directory and tsv sub directory
    */
  def extractAndConvert(
    v2fConstant: V2FConstants,
    context: ScioContext,
    inputDir: String,
    relativeFilePath: String
  ): SCollection[(String, JsonObject)] = {
    // get the readable files for the given input path
    val readableFiles = V2FUtils.getReadableFiles(
      s"$inputDir/${v2fConstant.tsvPattern}/$relativeFilePath",
      context
    )

    // then convert tsv to json and get the filepath
    V2FUtils.tsvToJson(
      v2fConstant.tableName
    )(readableFiles)
  }

  /**
    * Given conversion functions, for the field names specified, the fields of a provided JSON Object are converted based on the given functions.
    *
    * @param jsonAndFilePaths the collection of JSON Objects and associated file paths that will be transformed
    * @param v2fConstant the type of tsv(s) that will be transformed
    */
  def transform(
    jsonAndFilePaths: SCollection[(String, JsonObject)],
    v2fConstant: V2FConstants
  ): SCollection[(String, JsonObject)] = {
    // convert given fields to json double
    val transformedDoublesJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonDouble,
        V2FUtils.jsonStringToJsonDouble
      )(jsonAndFilePaths)

    // then convert given fields to json int
    val transformedIntsJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonInt,
        V2FUtils.jsonStringToJsonInt
      )(transformedDoublesJsonAndFilePaths)

    // then convert given fields to json booleans
    val transformedBooleansJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonBoolean,
        V2FUtils.jsonStringToJsonBoolean
      )(transformedIntsJsonAndFilePaths)

    // then convert given fields to json arrays
    val transformedArraysJsonAndFilePaths =
      v2fConstant.fieldsToConvertToJsonArray.foldLeft(
        transformedBooleansJsonAndFilePaths
      ) {
        case (currentTransformedJsonAndFilePaths, currentfieldsToConvertToJsonArray) =>
          V2FUtils.convertJsonFieldsValueType(
            v2fConstant.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FUtils.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJsonAndFilePaths)
      }

    // then convert given fields of the array from json strings to json double
    V2FUtils.convertJsonFieldsValueType(
      v2fConstant.tableName,
      v2fConstant.fieldsToConvertFromJsonArrayStringToDouble,
      V2FUtils.convertJsonArrayStringToDouble
    )(transformedArraysJsonAndFilePaths)
  }

  /**
    *  Write all the converted and transformed JSON Objects to disk.
    *
    * @param jsonAndFilePaths the collection of JSON Objects and associated file paths that will be saved as a JSON file
    * @param v2fConstant the type of tsv(s) that will be saved as a JSON file
    * @param outputDir the root outputs directory where the JSON file(s) will be saved
    */
  def writeToDisk(
    jsonAndFilePaths: SCollection[(String, JsonObject)],
    v2fConstant: V2FConstants,
    outputDir: String
  ): Unit = {
    jsonAndFilePaths.map {
      case (_, jsonObj) =>
        jsonObj
    }.saveAsJsonFile(
      s"$outputDir/${v2fConstant.tsvPattern}"
    )
    ()
  }
}
