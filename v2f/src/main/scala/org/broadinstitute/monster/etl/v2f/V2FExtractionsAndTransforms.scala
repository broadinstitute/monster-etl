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
    * Extracts variant JSON fields from a collection of JSON Objects and transforms selected field(s) from String to Long.
    *
    * @param v2fConstant the type of tsv(s) that will be extracted and converted to Json
    * @param jsonAndFilePaths tthe collection of JSON Objects and associated file paths that will be extracted and then transformed
    */
  def extractAndTransformVariants(
    v2fConstant: V2FConstants,
    jsonAndFilePaths: SCollection[(String, JsonObject)]
  ): SCollection[(String, JsonObject)] = {
    // extract the variant fields from the input JSON
    val variantEffectJsonAndFilePaths =
      V2FUtils.extractVariantFields(
        v2fConstant.tableName,
        v2fConstant.variantFieldsToExtract
      )(jsonAndFilePaths)

    // convert position from string to long
    V2FUtils.convertJsonFieldsValueType(
      v2fConstant.tableName,
      v2fConstant.fieldsToConvertToJsonLong,
      V2FUtils.jsonStringToJsonLong
    )(variantEffectJsonAndFilePaths)
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
    // rename given fields from old to new names
    val transformedRenamedFieldsJSON = V2FUtils.renameFields(
      v2fConstant.tableName,
      v2fConstant.fieldsToRename
    )(jsonAndFilePaths)

    // remove the given fields from the json object
    val transformedRemovedVariantFieldsJsonAndFilePaths =
      V2FUtils.removeFields(
        v2fConstant.tableName,
        v2fConstant.fieldsToRemove
      )(transformedRenamedFieldsJSON)

    // then convert given fields to json double
    val transformedDoublesJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonDouble,
        V2FUtils.jsonStringToJsonDouble
      )(transformedRemovedVariantFieldsJsonAndFilePaths)

    // then convert given fields to json Long
    val transformedLongsJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonLong,
        V2FUtils.jsonStringToJsonLong
      )(transformedDoublesJsonAndFilePaths)

    // then convert given fields to json booleans
    val transformedBooleansJsonAndFilePaths =
      V2FUtils.convertJsonFieldsValueType(
        v2fConstant.tableName,
        v2fConstant.fieldsToConvertToJsonBoolean,
        V2FUtils.jsonStringToJsonBoolean
      )(transformedLongsJsonAndFilePaths)

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

  /**
    *  Merge the variant JSON Objects and then write the merged JSON to disk.
    *
    * @param variantJsonAndFilePaths a list of the collections of JSON Objects and associated file paths that will be merged and then saved as a JSON file
    * @param outputDir the root outputs directory where the JSON file(s) will be saved
    */
  def mergeVariantJsonAndFilePathsAndWriteToDisk(
    variantJsonAndFilePaths: List[SCollection[(String, JsonObject)]],
    outputDir: String
  ): Unit = {
    SCollection
      .unionAll(variantJsonAndFilePaths.map { collection =>
        collection.map {
          case (_, jsonObj) =>
            jsonObj
        }
      })
      .saveAsJsonFile(
        s"$outputDir/variants"
      )
    ()
  }
}
