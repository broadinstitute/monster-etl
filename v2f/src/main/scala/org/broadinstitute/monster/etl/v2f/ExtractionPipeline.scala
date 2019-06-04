package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.{BuildInfo => _, io => _}
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

    // extract and convert TSVs to JSON, transform JSON and then save JSON
    // FrequencyAnalysis
    val frequencyAnalysisJsonAndFilePaths = V2FExtractionsAndTransforms.extractAndConvert(
      FrequencyAnalysis,
      pipelineContext,
      inputDir = parsedArgs.inputDir,
      relativeFilePath = "*/*.csv"
    )

    val frequencyAnalysisTransformedJsonAndFilePaths = V2FExtractionsAndTransforms
      .transform(frequencyAnalysisJsonAndFilePaths, FrequencyAnalysis)

    V2FExtractionsAndTransforms.writeToDisk(
      frequencyAnalysisTransformedJsonAndFilePaths,
      FrequencyAnalysis,
      parsedArgs.outputDir
    )

    // MetaAnalysisAncestrySpecific
    val metaAnalysisAncestrySpecificJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisAncestrySpecific,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*/*/*.csv"
      )

    val metaAnalysisAncestrySpecificTransformedJsonAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        metaAnalysisAncestrySpecificJsonAndFilePaths,
        MetaAnalysisAncestrySpecific
      )

    V2FExtractionsAndTransforms.writeToDisk(
      metaAnalysisAncestrySpecificTransformedJsonAndFilePaths,
      MetaAnalysisAncestrySpecific,
      parsedArgs.outputDir
    )

    // MetaAnalysisTransEthnic
    val metaAnalysisTransEthnicJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisTransEthnic,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*/*.csv"
      )

    val metaAnalysisTransEthnicTransformedJsonAndFilePaths = V2FExtractionsAndTransforms
      .transform(metaAnalysisTransEthnicJsonAndFilePaths, MetaAnalysisTransEthnic)

    V2FExtractionsAndTransforms.writeToDisk(
      metaAnalysisTransEthnicTransformedJsonAndFilePaths,
      MetaAnalysisTransEthnic,
      parsedArgs.outputDir
    )

    // VariantEffectRegulatoryFeatureConsequences
    val variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectRegulatoryFeatureConsequences,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*.csv"
      )

    val variantEffectRegulatoryFeatureConsequencesTransformedJsonAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths,
        VariantEffectRegulatoryFeatureConsequences
      )

    V2FExtractionsAndTransforms.writeToDisk(
      variantEffectRegulatoryFeatureConsequencesTransformedJsonAndFilePaths,
      VariantEffectRegulatoryFeatureConsequences,
      parsedArgs.outputDir
    )

    // VariantEffectTranscriptConsequences
    val variantEffectTranscriptConsequencesJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectTranscriptConsequences,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*.csv"
      )

    val variantEffectTranscriptConsequencesTransformedJsonAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        variantEffectTranscriptConsequencesJsonAndFilePaths,
        VariantEffectTranscriptConsequences
      )

    V2FExtractionsAndTransforms.writeToDisk(
      variantEffectTranscriptConsequencesTransformedJsonAndFilePaths,
      VariantEffectTranscriptConsequences,
      parsedArgs.outputDir
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
  }
}
