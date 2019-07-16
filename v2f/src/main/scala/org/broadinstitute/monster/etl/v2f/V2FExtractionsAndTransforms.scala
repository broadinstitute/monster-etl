package org.broadinstitute.monster.etl.v2f

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import org.broadinstitute.monster.etl.{MsgTransformations, UpackMsgCoder}
import upack.Msg

object V2FExtractionsAndTransforms {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

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
  ): SCollection[(String, Msg)] = {
    // get the readable files for the given input path
    val readableFiles = V2FUtils.getReadableFiles(
      s"$inputDir/${v2fConstant.filePath}/$relativeFilePath",
      context
    )

    // then convert tsv to msg and get the filepath
    V2FUtils.tsvToMsg(
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
    * @param v2fConstant the type of tsv(s) that will be transformed
    */
  def transform(
    v2fConstant: V2FConstants
  ): SCollection[(String, Msg)] => SCollection[(String, Msg)] = { jsonAndFilePaths =>
    jsonAndFilePaths.map {
      case (path, msg) =>
        val withSnakeCase = MsgTransformations.keysToSnakeCase(msg)
        val withRenamedFields =
          MsgTransformations.renameFields(v2fConstant.fieldsToRename)(withSnakeCase)
        val withDoubles = MsgTransformations.parseDoubles(
          Set(v2fConstant.fieldsToConvertToJsonDouble)
        )(withRenamedFields)
        val withLongs = MsgTransformations.parseLongs(
          Set(v2fConstant.fieldsToConvertToJsonLong)
        )(withDoubles)
        val withBooleans = MsgTransformations.parseBooleans(
          Set(v2fConstant.fieldsToConvertToJsonBoolean)
        )(withLongs)
        val withBooleans = MsgTransformations.parseBooleans(
          Set(v2fConstant.fieldsToConvertToJsonBoolean)
        )(withLongs)
    }
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
    *  Merge the variant JSON Objects and then write the merged JSON to disk.
    *
    * @param variantJsonAndFilePaths a list of the collections of JSON Objects and associated file paths that will be merged and then saved as a JSON file
    */
  def mergeVariantJsons(
    variantJsonAndFilePaths: List[SCollection[(String, JsonObject)]]
  ): SCollection[JsonObject] = {
    SCollection
      .unionAll(variantJsonAndFilePaths.map { collection =>
        collection.map {
          case (_, jsonObj) =>
            jsonObj
        }
      })
      .distinctBy(
        _.apply("id")
          .flatMap(_.asString)
          .getOrElse(throw new RuntimeException("Got variant without an ID!"))
      )
  }
}
