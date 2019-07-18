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
    * Extracts variant Msg fields from a collection of Msg Objects and transforms selected field(s) from String to Long.
    *
    * @param v2fConstant the type of tsv(s) that will be extracted and converted to Json
    * @param msgAndFilePaths tthe collection of Msg Objects and associated file paths that will be extracted and then transformed
    */
  def extractAndTransformVariants(
    v2fConstant: V2FConstants,
    msgAndFilePaths: SCollection[(String, Msg)]
  ): SCollection[(String, Msg)] = {
    // extract the variant fields from the input JSON
    val variantEffectJsonAndFilePaths =
      V2FUtils.extractVariantFields(
        v2fConstant.tableName,
        v2fConstant.variantFieldsToExtract
      )(msgAndFilePaths)

    // convert position from string to long
    V2FUtils.convertJsonFieldsValueType(
      v2fConstant.tableName,
      v2fConstant.fieldsToConvertToMsgLong,
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
  ): SCollection[(String, Msg)] => SCollection[(String, Msg)] = { msgAndFilePaths =>
    msgAndFilePaths.map {
      case (path, msg) =>
        val withSnakeCase = MsgTransformations.keysToSnakeCase(msg)
        // rename fields
        val withRenamedFields =
          MsgTransformations.renameFields(v2fConstant.fieldsToRename)(withSnakeCase)
        // need to remove fields
        val withRemovedFields =
          MsgTransformations.removeFields(v2fConstant.fieldsToRemove)(withRenamedFields)
        // convert fields from string to double
        val withDoubles = MsgTransformations.parseDoubles(
          v2fConstant.fieldsToConvertToMsgDouble
        )(withRemovedFields)
        // convert fields from string to long
        val withLongs = MsgTransformations.parseLongs(
          v2fConstant.fieldsToConvertToMsgLong
        )(withDoubles)
        // convert fields from string to boolean
        val withBooleans = MsgTransformations.parseBooleans(
          v2fConstant.fieldsToConvertToMsgBoolean
        )(withLongs)
        // convert to arrays, then convert fields to double
        MsgTransformations.parseDoubleArrays(
          v2fConstant.fieldsToConvertToMsgArray._2,
          v2fConstant.fieldsToConvertToMsgArray._1
        )(withBooleans)
    }

    // then convert given fields to json arrays
    val transformedArraysJsonAndFilePaths =
      v2fConstant.fieldsToConvertToMsgArray.foldLeft(
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
      v2fConstant.fieldsToConvertFromMsgArrayStringToDouble,
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
