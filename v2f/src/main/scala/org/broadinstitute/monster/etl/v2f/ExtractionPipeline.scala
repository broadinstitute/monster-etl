package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.{BuildInfo => _, io => _}
import com.spotify.scio.extra.json._
import org.broadinstitute.monster.etl.BuildInfo

/**
  * ETL workflow for converting and transforming TSVs from V2F.
  */
object ExtractionPipeline {

  @AppName("v2f Extraction Pipeline")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.v2f.ExtractionPipeline")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
    @HelpMessage("Directory containing analysis TSV for V2F")
    inputDir: String,
    @HelpMessage("Path of directory where the processed V2F JSON should be written")
    outputDir: String
  )

  /**
    * Convert V2F TSVs to JSON and performing transformations for ingest into the Data Repository.
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // convert the TSVs to JSON
    // frequency analysis
    val frequencyAnalysisReadableFiles = V2FUtils.getReadableFiles(
      s"${parsedArgs.inputDir}/${FrequencyAnalysis.tsvPattern}/*/*.csv",
      pipelineContext
    )

    val frequencyAnalysisJsonAndFilePaths = V2FUtils.tsvToJson(
      FrequencyAnalysis.tableName
    )(frequencyAnalysisReadableFiles)

    // meta analysis ancestry specific
    val metaAnalysisAncestrySpecificReadableFiles = V2FUtils.getReadableFiles(
      s"${parsedArgs.inputDir}/${MetaAnalysisAncestrySpecific.tsvPattern}/*/*/*.csv",
      pipelineContext
    )

    val metaAnalysisAncestrySpecificJsonAndFilePaths = V2FUtils.tsvToJson(
      MetaAnalysisAncestrySpecific.tableName
    )(metaAnalysisAncestrySpecificReadableFiles)

    // meta analysis trans ethnic
    val metaAnalysisTransEthnicReadableFiles = V2FUtils.getReadableFiles(
      s"${parsedArgs.inputDir}/${MetaAnalysisTransEthnic.tsvPattern}/*/*.csv",
      pipelineContext
    )

    val metaAnalysisTransEthnicJsonAndFilePaths = V2FUtils.tsvToJson(
      MetaAnalysisTransEthnic.tableName
    )(metaAnalysisTransEthnicReadableFiles)

    // variant effect regulatory feature consequences
    val variantEffectRegulatoryFeatureConsequencesReadableFiles =
      V2FUtils.getReadableFiles(
        s"${parsedArgs.inputDir}/${VariantEffectRegulatoryFeatureConsequences.tsvPattern}/*.csv",
        pipelineContext
      )

    val variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths =
      V2FUtils.tsvToJson(
        VariantEffectRegulatoryFeatureConsequences.tableName
      )(variantEffectRegulatoryFeatureConsequencesReadableFiles)

    // variant effect regulatory feature consequences
    val variantEffectTranscriptConsequencesReadableFiles = V2FUtils.getReadableFiles(
      s"${parsedArgs.inputDir}/${VariantEffectTranscriptConsequences.tsvPattern}/*.csv",
      pipelineContext
    )

    val variantEffectTranscriptConsequencesJsonAndFilePaths = V2FUtils.tsvToJson(
      VariantEffectTranscriptConsequences.tableName
    )(variantEffectTranscriptConsequencesReadableFiles)

    // transform json by adding fields and enforcing types
    // convert given fields to json double
    val frequencyAnalysisTransformed = V2FUtils.convertJsonFieldsValueType(
      FrequencyAnalysis.tableName,
      FrequencyAnalysis.fieldsToConvertToJsonDouble,
      V2FUtils.jsonStringToJsonDouble
    )(frequencyAnalysisJsonAndFilePaths)

    // add ancestry id to the json
    val metaAnalysisAncestrySpecificAddedFields = V2FUtils.addAncestryID(
      MetaAnalysisAncestrySpecific.tableName
    )(metaAnalysisAncestrySpecificJsonAndFilePaths)

    // convert given fields to json double
    val metaAnalysisAncestrySpecificTransformedDoubles =
      V2FUtils.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonDouble,
        V2FUtils.jsonStringToJsonDouble
      )(metaAnalysisAncestrySpecificAddedFields)

    // convert given fields to json int
    val metaAnalysisAncestrySpecificTransformed =
      V2FUtils.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonInt,
        V2FUtils.jsonStringToJsonInt
      )(metaAnalysisAncestrySpecificTransformedDoubles)

    // convert given fields to json double
    val metaAnalysisTransEthnicTransformedDoubles =
      V2FUtils.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonDouble,
        V2FUtils.jsonStringToJsonDouble
      )(metaAnalysisTransEthnicJsonAndFilePaths)

    // convert given fields to json int
    val metaAnalysisTransEthnicTransformed =
      V2FUtils.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonInt,
        V2FUtils.jsonStringToJsonInt
      )(metaAnalysisTransEthnicTransformedDoubles)

    // convert given fields to json boolean
    val variantEffectRegulatoryFeatureConsequencesTransformedBooleans =
      V2FUtils.convertJsonFieldsValueType(
        VariantEffectRegulatoryFeatureConsequences.tableName,
        VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonBoolean,
        V2FUtils.jsonStringToJsonBoolean
      )(variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths)

    // then convert given fields to json arrays
    val variantEffectRegulatoryFeatureConsequencesTransformed =
      VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectRegulatoryFeatureConsequencesTransformedBooleans
      ) {
        case (currentTransformedJsonAndFilePaths, currentfieldsToConvertToJsonArray) =>
          V2FUtils.convertJsonFieldsValueType(
            VariantEffectRegulatoryFeatureConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FUtils.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJsonAndFilePaths)
      }

    // convert given fields to json double
    val variantEffectTranscriptConsequencesTransformedDoubles =
      V2FUtils.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonDouble,
        V2FUtils.jsonStringToJsonDouble
      )(variantEffectTranscriptConsequencesJsonAndFilePaths)

    // convert given fields to json int
    val variantEffectTranscriptConsequencesTransformedInts =
      V2FUtils.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonInt,
        V2FUtils.jsonStringToJsonInt
      )(variantEffectTranscriptConsequencesTransformedDoubles)

    // then convert given fields to json booleans
    val variantEffectTranscriptConsequencesTransformedBooleans =
      V2FUtils.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonBoolean,
        V2FUtils.jsonStringToJsonBoolean
      )(variantEffectTranscriptConsequencesTransformedInts)

    // then convert given fields to json arrays
    val variantEffectTranscriptConsequencesTransformedArrays =
      VariantEffectTranscriptConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectTranscriptConsequencesTransformedBooleans
      ) {
        case (currentTransformedJsonAndFilePaths, currentfieldsToConvertToJsonArray) =>
          V2FUtils.convertJsonFieldsValueType(
            VariantEffectTranscriptConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FUtils.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJsonAndFilePaths)
      }

    // then convert given fields of the array from json strings to json double
    val variantEffectTranscriptConsequencesTransformed =
      V2FUtils.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertFromJsonArrayStringToDouble,
        V2FUtils.convertJsonArrayStringToDouble
      )(variantEffectTranscriptConsequencesTransformedArrays)

    // Write all the converted/parsed json objects to disk.
    frequencyAnalysisTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${FrequencyAnalysis.tsvPattern}"
    )

    metaAnalysisAncestrySpecificTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${MetaAnalysisAncestrySpecific.tsvPattern}"
    )

    metaAnalysisTransEthnicTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${MetaAnalysisTransEthnic.tsvPattern}"
    )

    variantEffectRegulatoryFeatureConsequencesTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${VariantEffectRegulatoryFeatureConsequences.tsvPattern}"
    )

    variantEffectTranscriptConsequencesTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${VariantEffectTranscriptConsequences.tsvPattern}"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
  }
}
