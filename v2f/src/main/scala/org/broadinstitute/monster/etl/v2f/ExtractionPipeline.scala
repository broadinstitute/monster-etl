package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{BuildInfo => _, io => _}
import io.circe.JsonObject
import org.broadinstitute.monster.etl._
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
    val frequencyAnalysisMsgAndFilePaths = V2FExtractionsAndTransforms.extractAndConvert(
      FrequencyAnalysis,
      pipelineContext,
      inputDir = parsedArgs.inputDir,
      relativeFilePath = "*/*.csv"
    )

    val frequencyAnalysisTransformedMsgAndFilePaths = V2FExtractionsAndTransforms
      .transform(FrequencyAnalysis)(frequencyAnalysisMsgAndFilePaths)

    // MetaAnalysisAncestrySpecific
    val metaAnalysisAncestrySpecificMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisAncestrySpecific,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*/*/*.csv"
      )

    val metaAnalysisAncestrySpecificTransformedMsgAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        MetaAnalysisAncestrySpecific
      )(metaAnalysisAncestrySpecificMsgAndFilePaths)

    val metaAnalysisAncestrySpecificTransformedMsgAndFilePathsAndAncestryID =
      V2FUtils.addAncestryID(MetaAnalysisAncestrySpecific.tableName)(
        metaAnalysisAncestrySpecificTransformedMsgAndFilePaths
      )

    // MetaAnalysisTransEthnic
    val metaAnalysisTransEthnicMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisTransEthnic,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*/*.csv"
      )

    val metaAnalysisTransEthnicTransformedMsgAndFilePaths = V2FExtractionsAndTransforms
      .transform(MetaAnalysisTransEthnic)(metaAnalysisTransEthnicMsgAndFilePaths)

    // VariantEffectRegulatoryFeatureConsequences
    val variantEffectRegulatoryFeatureConsequencesMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectRegulatoryFeatureConsequences,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*.csv"
      )

    val variantEffectRegulatoryFeatureConsequencesTransformedMsgAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        VariantEffectRegulatoryFeatureConsequences
      )(variantEffectRegulatoryFeatureConsequencesMsgAndFilePaths)

    // VariantEffectTranscriptConsequences
    val variantEffectTranscriptConsequencesMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectTranscriptConsequences,
        pipelineContext,
        inputDir = parsedArgs.inputDir,
        relativeFilePath = "*.csv"
      )

    val variantEffectTranscriptConsequencesTransformedMsgAndFilePaths =
      V2FExtractionsAndTransforms.transform(
        VariantEffectTranscriptConsequences
      )(variantEffectTranscriptConsequencesMsgAndFilePaths)

    // variant Msgs
    val frequencyAnalysisVariantMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        FrequencyAnalysis,
        frequencyAnalysisMsgAndFilePaths
      )

    val metaAnalysisTransEthnicVariantMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisTransEthnic,
        metaAnalysisTransEthnicMsgAndFilePaths
      )

    val metaAnalysisAncestrySpecificVariantMsgAndFilePaths =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisAncestrySpecific,
        metaAnalysisAncestrySpecificMsgAndFilePaths
      )

    val variantMergedMsg =
      V2FExtractionsAndTransforms.mergeVariantMsgs(
        List(
          frequencyAnalysisVariantMsgAndFilePaths,
          metaAnalysisAncestrySpecificVariantMsgAndFilePaths,
          metaAnalysisTransEthnicVariantMsgAndFilePaths
        )
      )

    // save the extracted and transformed Msgs
    writeToDisk(
      frequencyAnalysisTransformedMsgAndFilePaths,
      FrequencyAnalysis.tableName,
      filePath = FrequencyAnalysis.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      variantEffectTranscriptConsequencesTransformedMsgAndFilePaths,
      VariantEffectTranscriptConsequences.tableName,
      filePath = VariantEffectTranscriptConsequences.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      metaAnalysisAncestrySpecificTransformedMsgAndFilePathsAndAncestryID,
      MetaAnalysisAncestrySpecific.tableName,
      filePath = MetaAnalysisAncestrySpecific.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      metaAnalysisTransEthnicTransformedMsgAndFilePaths,
      MetaAnalysisTransEthnic.tableName,
      filePath = MetaAnalysisTransEthnic.filePath,
      parsedArgs.outputDir
    )

    writeToDisk(
      variantEffectRegulatoryFeatureConsequencesTransformedMsgAndFilePaths,
      VariantEffectRegulatoryFeatureConsequences.tableName,
      filePath = VariantEffectRegulatoryFeatureConsequences.filePath,
      parsedArgs.outputDir
    )

    MsgIO.writeJsonLists(
      variantMergedMsg,
      "Variants",
      s"${parsedArgs.outputDir}/variants"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close() //.waitUntilDone()
    ()
  }

  /**
    *  Write all the converted and transformed Msg Objects to disk.
    *
    * @param msgAndFilePaths the collection of Msg Objects and associated file paths that will be saved as a JSON file
    * @param filePath File pattern matching TSVs to process within the V2F analysis directory
    * @param outputDir the root outputs directory where the JSON file(s) will be saved
    */
  def writeToDisk(
    msgAndFilePaths: SCollection[(String, Msg)],
    description: String,
    filePath: String,
    outputDir: String
  ): Unit = {
    MsgIO.writeJsonLists(
      msgAndFilePaths.map { case (_, msgObj) => msgObj },
      description,
      s"$outputDir/$filePath"
    )
    ()
  }
}
