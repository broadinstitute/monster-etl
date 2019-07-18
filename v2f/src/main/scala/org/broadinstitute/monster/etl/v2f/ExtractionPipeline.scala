package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{BuildInfo => _, io => _}
import io.circe.JsonObject
import org.broadinstitute.monster.etl._
import com.spotify.scio.extra.json._
import upack.Msg

/**
  * ETL workflow for converting and transforming TSVs from V2F.
  */
object ExtractionPipeline {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("V2F Extraction Pipeline")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.v2f.ExtractionPipeline")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * Scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
    @HelpMessage("Directory containing analysis TSV for V2F")
    inputDir: String,
    @HelpMessage("Path of directory where the processed V2F JSON should be written")
    outputDir: String
  )

  /**
    * Convert V2F TSVs to Msg and performing transformations for ingest into the Data Repository.
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // extract and convert TSVs to Msg, transform Msg and then save Msg
    // FrequencyAnalysis
    val frequencyAnalysisJsonAndFilePaths = V2FExtractionsAndTransforms.extractAndConvert(
      FrequencyAnalysis,
      pipelineContext,
      inputDir = parsedArgs.inputDir,
      relativeFilePath = "*/*.csv"
    )

    val frequencyAnalysisTransformedJsonAndFilePaths = V2FExtractionsAndTransforms
      .transform(FrequencyAnalysis)(frequencyAnalysisJsonAndFilePaths)

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
        MetaAnalysisAncestrySpecific
      )(metaAnalysisAncestrySpecificJsonAndFilePaths)

    // MetaAnalysisTransEthnic
    val metaAnalysisTransEthnicJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisTransEthnic,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*/*.csv"
      )

    val metaAnalysisTransEthnicTransformedJsonAndFilePaths = V2FExtractionsAndTransforms
      .transform(MetaAnalysisTransEthnic)(metaAnalysisTransEthnicJsonAndFilePaths)

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
        VariantEffectRegulatoryFeatureConsequences
      )(variantEffectRegulatoryFeatureConsequencesJsonAndFilePaths)

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
        VariantEffectTranscriptConsequences
      )(variantEffectTranscriptConsequencesJsonAndFilePaths)

    // variant Msgs
    val frequencyAnalysisVariantJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        FrequencyAnalysis,
        frequencyAnalysisJsonAndFilePaths
      )

    val metaAnalysisTransEthnicVariantJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisTransEthnic,
        metaAnalysisTransEthnicJsonAndFilePaths
      )

    val metaAnalysisAncestrySpecificVariantJsonAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisAncestrySpecific,
        metaAnalysisAncestrySpecificJsonAndFilePaths
      )

    val variantMergedJson =
      V2FExtractionsAndTransforms.mergeVariantMsgs(
        List(
          frequencyAnalysisVariantJsonAndFilePaths,
          metaAnalysisAncestrySpecificVariantJsonAndFilePaths,
          metaAnalysisTransEthnicVariantJsonAndFilePaths
        )
      )

    // save the extracted and transformed Msgs
    writeToDisk(
      frequencyAnalysisTransformedJsonAndFilePaths,
      filePath = FrequencyAnalysis.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      variantEffectTranscriptConsequencesTransformedJsonAndFilePaths,
      filePath = VariantEffectTranscriptConsequences.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      metaAnalysisAncestrySpecificTransformedJsonAndFilePaths,
      filePath = MetaAnalysisAncestrySpecific.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      metaAnalysisTransEthnicTransformedJsonAndFilePaths,
      filePath = MetaAnalysisTransEthnic.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      variantEffectRegulatoryFeatureConsequencesTransformedJsonAndFilePaths,
      filePath = VariantEffectRegulatoryFeatureConsequences.filePath,
      parsedArgs.outputDir
    )

    variantMergedJson.saveAsJsonFile(
      s"${parsedArgs.outputDir}/variants"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close() //.waitUntilDone()
    ()
  }

  /**
    *  Write all the converted and transformed Msg Objects to disk.
    *
    * @param msgAndFilePaths the collection of Msg Objects and associated file paths that will be saved as a Msg file
    * @param filePath File pattern matching TSVs to process within the V2F analysis directory
    * @param outputDir the root outputs directory where the Msg file(s) will be saved
    */
  def writeToDisk(
    msgAndFilePaths: SCollection[(String, Msg)],
    filePath: String,
    outputDir: String
  ): Unit = {
    msgAndFilePaths.map {
      case (_, msgObj) =>
        msgObj
    }.saveAsJsonFile(
      s"$outputDir/$filePath"
    )
    ()
  }
}
