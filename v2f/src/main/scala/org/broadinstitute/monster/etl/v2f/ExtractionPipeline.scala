package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.{BuildInfo => _, io => _}
import com.spotify.scio.extra.json._
import org.broadinstitute.monster.etl.BuildInfo

/**
  * ETL workflow for converting and tranforming tsv's from v2f.
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
    @HelpMessage("Directory containing analysis tsv's for v2f")
    inputDir: String,
    @HelpMessage("Path of directory where the extracted raw v2f json should be written")
    outputDir: String
  )

  /**
    * pulling in v2f tsv's and converting them to json and performing basic transformations
    * given the command line arguments
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // get tsvPaths and converting tsv's to json.
    // frequency analysis
    val frequencyAnalysisTSVPath = V2FExtractions.getReadableFile(
      s"${parsedArgs.inputDir}/${FrequencyAnalysis.tsvPath}",
      pipelineContext
    )

    val frequencyAnalysisJsonAndFilePaths = V2FExtractions.tsvToJson(
      FrequencyAnalysis.tableName
    )(frequencyAnalysisTSVPath)

    // meta analysis ancestry specific
    val metaAnalysisAncestrySpecificReadableFile = V2FExtractions.getReadableFile(
      s"${parsedArgs.inputDir}/${MetaAnalysisAncestrySpecific.tsvPath}",
      pipelineContext
    )

    val metaAnalysisAncestrySpecificJsonAndFilePaths = V2FExtractions.tsvToJson(
      MetaAnalysisAncestrySpecific.tableName
    )(metaAnalysisAncestrySpecificReadableFile)

    // meta analysis trans ethnic
    val metaAnalysisTransEthnicReadableFile = V2FExtractions.getReadableFile(
      s"${parsedArgs.inputDir}/${MetaAnalysisTransEthnic.tsvPath}",
      pipelineContext
    )

    val metaAnalysisTransEthnicJsonAndFilePaths = V2FExtractions.tsvToJson(
      MetaAnalysisTransEthnic.tableName
    )(metaAnalysisTransEthnicReadableFile)

    // variant effect regulatory feature consequences
    val variantEffectRegulatoryFeatureConsequencesReadableFile =
      V2FExtractions.getReadableFile(
        s"${parsedArgs.inputDir}/${VariantEffectRegulatoryFeatureConsequences.tsvPath}",
        pipelineContext
      )

    val variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths =
      V2FExtractions.tsvToJson(
        VariantEffectRegulatoryFeatureConsequences.tableName
      )(variantEffectRegulatoryFeatureConsequencesReadableFile)

    // variant effect regulatory feature consequences
    val variantEffectTranscriptConsequencesReadableFile = V2FExtractions.getReadableFile(
      s"${parsedArgs.inputDir}/${VariantEffectTranscriptConsequences.tsvPath}",
      pipelineContext
    )

    val variantEffectTranscriptConsequencesJsonAndFilePaths = V2FExtractions.tsvToJson(
      VariantEffectTranscriptConsequences.tableName
    )(variantEffectTranscriptConsequencesReadableFile)

    // transform json by adding fields and enforcing types
    // convert given fields to json double
    val frequencyAnalysisTransformed = V2FExtractions.convertJsonFieldsValueType(
      FrequencyAnalysis.tableName,
      FrequencyAnalysis.fieldsToConvertToJsonDouble,
      V2FExtractions.jsonStringToJsonDouble
    )(frequencyAnalysisJsonAndFilePaths)

    // add ancestry id to the json
    val metaAnalysisAncestrySpecificAddedFields = V2FExtractions.addAncestryID(
      MetaAnalysisAncestrySpecific.tableName
    )(metaAnalysisAncestrySpecificJsonAndFilePaths)

    // convert given fields to json double
    val metaAnalysisAncestrySpecificTransformedDoubles =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(metaAnalysisAncestrySpecificAddedFields)

    // convert given fields to json int
    val metaAnalysisAncestrySpecificTransformed =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(metaAnalysisAncestrySpecificTransformedDoubles)

    // convert given fields to json double
    val metaAnalysisTransEthnicTransformedDoubles =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(metaAnalysisTransEthnicJsonAndFilePaths)

    // convert given fields to json int
    val metaAnalysisTransEthnicTransformed =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(metaAnalysisTransEthnicTransformedDoubles)

    // convert given fields to json boolean
    val variantEffectRegulatoryFeatureConsequencesTransformedBools =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectRegulatoryFeatureConsequences.tableName,
        VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonBoolean,
        V2FExtractions.jsonStringToJsonBoolean
      )(variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths)

    // then convert given fields to json arrays
    val variantEffectRegulatoryFeatureConsequencesTransformed =
      VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectRegulatoryFeatureConsequencesTransformedBools
      ) {
        case (currentTransformedJsonAndFilePaths, currentfieldsToConvertToJsonArray) =>
          V2FExtractions.convertJsonFieldsValueType(
            VariantEffectRegulatoryFeatureConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FExtractions.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJsonAndFilePaths)
      }

    // convert given fields to json double
    val variantEffectTranscriptConsequencesTransformedDoubles =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(variantEffectTranscriptConsequencesJsonAndFilePaths)

    // convert given fields to json int
    val variantEffectTranscriptConsequencesTransformedInts =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(variantEffectTranscriptConsequencesTransformedDoubles)

    // then convert given fields to json booleans
    val variantEffectTranscriptConsequencesTransformedBools =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonBoolean,
        V2FExtractions.jsonStringToJsonBoolean
      )(variantEffectTranscriptConsequencesTransformedInts)

    // then convert given fields to json arrays
    val variantEffectTranscriptConsequencesTransformedArrays =
      VariantEffectTranscriptConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectTranscriptConsequencesTransformedBools
      ) {
        case (currentTransformedJsonAndFilePaths, currentfieldsToConvertToJsonArray) =>
          V2FExtractions.convertJsonFieldsValueType(
            VariantEffectTranscriptConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FExtractions.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJsonAndFilePaths)
      }

    // then convert given fields of the array from json strings to json double
    val variantEffectTranscriptConsequencesTransformed =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertFromJsonArrayStringToDouble,
        V2FExtractions.convertJsonArrayStringToDouble
      )(variantEffectTranscriptConsequencesTransformedArrays)

    // Write all the converted/parsed json objects to disk.
    frequencyAnalysisTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/frequencyAnalysis"
    )

    metaAnalysisAncestrySpecificTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/metaanalysis/ancestry-ethnic"
    )

    metaAnalysisTransEthnicTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/metaanalysis/trans-specific"
    )

    variantEffectRegulatoryFeatureConsequencesTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/variantEffect/regulatory_feature_consequences"
    )

    variantEffectTranscriptConsequencesTransformed.map {
      case (jsonObj, _) =>
        jsonObj
    }.saveAsJsonFile(
      s"${parsedArgs.outputDir}/variantEffect/transcript_consequences"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
  }
}
