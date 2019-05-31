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

    // converting tsv's to json.
    val frequencyAnalysisJson =
      V2FExtractions.tsvToJson(
        s"${parsedArgs.inputDir}/frequencyanalysis/*/*",
        pipelineContext
      )

    val metaAnalysisAncestrySpecificJson = V2FExtractions.tsvToJson(
      s"${parsedArgs.inputDir}/metaanalysis/ancestry-specific/*/*/*",
      pipelineContext
    )

    val metaAnalysisTransEthnicJson =
      V2FExtractions.tsvToJson(
        s"${parsedArgs.inputDir}/metaanalysis/trans-ethnic/*/*",
        pipelineContext
      )

    val variantEffectRegulatoryFeatureConsequencesJson = V2FExtractions.tsvToJson(
      s"${parsedArgs.inputDir}/varianteffect/regulatory_feature_consequences/*",
      pipelineContext
    )

    val variantEffectTranscriptConsequencesJson = V2FExtractions.tsvToJson(
      s"${parsedArgs.inputDir}/varianteffect/transcript_consequences/*",
      pipelineContext
    )

    // transform json by adding fields and enforcing types
    // convert given fields to json double
    val frequencyAnalysisTransformedJson = V2FExtractions.convertJsonFieldsValueType(
      FrequencyAnalysis.tableName,
      FrequencyAnalysis.fieldsToConvertToJsonDouble,
      V2FExtractions.jsonStringToJsonDouble
    )(frequencyAnalysisJson)

    // convert given fields to json double
    val metaAnalysisAncestrySpecificTransformedDoubleJson =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(metaAnalysisAncestrySpecificJson)

    // convert given fields to json int
    val metaAnalysisAncestrySpecificTransformedJson =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisAncestrySpecific.tableName,
        MetaAnalysisAncestrySpecific.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(metaAnalysisAncestrySpecificTransformedDoubleJson)

    // convert given fields to json double
    val metaAnalysisTransEthnicTransformedDoubleJson =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(metaAnalysisTransEthnicJson)

    // convert given fields to json int
    val metaAnalysisTransEthnicTransformedJson =
      V2FExtractions.convertJsonFieldsValueType(
        MetaAnalysisTransEthnic.tableName,
        MetaAnalysisTransEthnic.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(metaAnalysisTransEthnicTransformedDoubleJson)

    // convert given fields to json boolean
    val variantEffectRegulatoryFeatureConsequencesTransformedBoolsJson =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectRegulatoryFeatureConsequences.tableName,
        VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonBoolean,
        V2FExtractions.jsonStringToJsonBoolean
      )(variantEffectRegulatoryFeatureConsequencesJson)

    // then convert given fields to json arrays
    val variantEffectRegulatoryFeatureConsequencesTransformedJson =
      VariantEffectRegulatoryFeatureConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectRegulatoryFeatureConsequencesTransformedBoolsJson
      ) {
        case (currentTransformedJson, currentfieldsToConvertToJsonArray) =>
          V2FExtractions.convertJsonFieldsValueType(
            VariantEffectRegulatoryFeatureConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FExtractions.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJson)
      }

    // convert given fields to json double
    val variantEffectTranscriptConsequencesTransformedDoubleJson =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonDouble,
        V2FExtractions.jsonStringToJsonDouble
      )(variantEffectTranscriptConsequencesJson)

    // convert given fields to json int
    val variantEffectTranscriptConsequencesTransformedIntJson =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonInt,
        V2FExtractions.jsonStringToJsonInt
      )(variantEffectTranscriptConsequencesTransformedDoubleJson)

    // then convert given fields to json booleans
    val variantEffectTranscriptConsequencesTransformedBooleansJson =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertToJsonBoolean,
        V2FExtractions.jsonStringToJsonBoolean
      )(variantEffectTranscriptConsequencesTransformedIntJson)

    // then convert given fields to json arrays
    val variantEffectTranscriptConsequencesTransformedArraysJson =
      VariantEffectTranscriptConsequences.fieldsToConvertToJsonArray.foldLeft(
        variantEffectTranscriptConsequencesTransformedBooleansJson
      ) {
        case (currentTransformedJson, currentfieldsToConvertToJsonArray) =>
          V2FExtractions.convertJsonFieldsValueType(
            VariantEffectTranscriptConsequences.tableName,
            currentfieldsToConvertToJsonArray._2,
            V2FExtractions.jsonStringToJsonArray(
              delimeter = currentfieldsToConvertToJsonArray._1
            )
          )(currentTransformedJson)
      }

    // then convert given fields of the array from json strings to json double
    val variantEffectTranscriptConsequencesTransformedJson =
      V2FExtractions.convertJsonFieldsValueType(
        VariantEffectTranscriptConsequences.tableName,
        VariantEffectTranscriptConsequences.fieldsToConvertFromJsonArrayStringToDouble,
        V2FExtractions.convertJsonArrayStringToDouble
      )(variantEffectTranscriptConsequencesTransformedArraysJson)

    // Write all the converted/parsed JSON to disk.
    frequencyAnalysisTransformedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/frequencyAnalysis"
    )
    metaAnalysisAncestrySpecificTransformedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/metaanalysis/trans-ethnic"
    )
    metaAnalysisTransEthnicTransformedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/metaanalysis/ancestry-specific"
    )
    variantEffectRegulatoryFeatureConsequencesTransformedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/variantEffect/regulatory_feature_consequences"
    )
    variantEffectTranscriptConsequencesTransformedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/variantEffect/transcript_consequences"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
  }
}
