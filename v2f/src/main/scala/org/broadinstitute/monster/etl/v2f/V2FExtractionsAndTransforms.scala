package org.broadinstitute.monster.etl.v2f

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.etl.{MsgTransformations, UpackMsgCoder}
import upack.{Msg, Str}

object V2FExtractionsAndTransforms {

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
    V2FUtils.tsvToMsg(v2fConstant.tableName)(readableFiles)
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
    msgAndFilePaths.map {
      case (path, msg) =>
        // change to snake case
        val withSnakeCase = MsgTransformations.keysToSnakeCase(msg)
        // rename fields
        val withRenamedFields =
          MsgTransformations.renameFields(Map("var_id" -> "id"))(withSnakeCase)
        // extract variant fields
        val withExtractedFields =
          MsgTransformations.extractFields(v2fConstant.variantFieldsToExtract)(
            withRenamedFields
          )
        // convert to longs
        val withLongs =
          MsgTransformations.parseLongs(v2fConstant.fieldsToConvertToMsgLong)(
            withExtractedFields
          )
        // return final
        (path, withLongs)
    }
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
        // convert to double arrays
        val withStringArrays =
          v2fConstant.fieldsToConvertToStringArray.foldLeft(withBooleans) {
            case (currentMsg, (delimeter, fields)) =>
              MsgTransformations.parseStringArrays(
                fields,
                delimeter
              )(currentMsg)
          }
        // convert to double arrays
        val withDoubleArrays =
          v2fConstant.fieldsToConvertToDoubleArray.foldLeft(withStringArrays) {
            case (currentMsg, (delimeter, fields)) =>
              MsgTransformations.parseDoubleArrays(
                fields,
                delimeter,
                Set(".")
              )(currentMsg)
          }
        // return final Msg
        (path, withDoubleArrays)
    }
  }

  /**
    *  Merge the variant Msg Objects and then write the merged Msg to disk.
    *
    * @param variantMsgAndFilePaths a list of the collections of Msg Objects and associated file paths that will be merged and then saved as a Msg file
    */
  def mergeVariantMsgs(
    variantMsgAndFilePaths: List[SCollection[(String, Msg)]]
  ): SCollection[Msg] = {
    SCollection
      .unionAll(variantMsgAndFilePaths.map { collection =>
        collection.map {
          case (_, msgObj) =>
            msgObj
        }
      })
      .distinctBy(_.obj.apply(Str("id")).str)
  }
}
