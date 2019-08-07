package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.{ContextAndArgs, ScioContext, BuildInfo => _, io => _}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.etl._
import upack.Msg

/**
  * ETL workflow for converting and transforming TSVs from V2F.
  */
object ExtractionPipeline {

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

    val inputDir = parsedArgs.inputDir
    val outputDir = parsedArgs.outputDir

    // extract and convert TSVs to Msg, transform Msg and then save Msg
    // FrequencyAnalysis
    convertAndWrite(pipelineContext, inputDir, outputDir).close()
    () // return Unit type
  }

  /**
    * Convert V2F TSVs to Msg and perform necessary transformations.
    *
    * @param pipelineContext the ScioContext in which to run this pipeline.
    * @param inputDir the directory from which to read data.
    * @param outputDir the directory in which to write data.
    */
  def convertAndWrite(
    pipelineContext: ScioContext,
    inputDir: String,
    outputDir: String
  ): ScioContext = {
    val faExtractedAndConverted = V2FExtractionsAndTransforms.extractAndConvert(
      FrequencyAnalysis,
      pipelineContext,
      inputDir = inputDir,
      relativeFilePath = "**.csv"
    )

    val faTransformed = V2FExtractionsAndTransforms
      .transform(FrequencyAnalysis)(faExtractedAndConverted)

    // MetaAnalysisAncestrySpecific
    val maasExtractedAndConverted =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisAncestrySpecific,
        pipelineContext,
        inputDir = inputDir,
        relativeFilePath = "***.csv"
      )

    val maasTransformed =
      V2FExtractionsAndTransforms.transform(
        MetaAnalysisAncestrySpecific
      )(maasExtractedAndConverted)

    val maasTransformedAndAncestryID =
      V2FUtils.addAncestryID(MetaAnalysisAncestrySpecific.tableName)(
        maasTransformed
      )

    // MetaAnalysisTransEthnic
    val mateExtractedAndConverted =
      V2FExtractionsAndTransforms.extractAndConvert(
        MetaAnalysisTransEthnic,
        pipelineContext,
        inputDir = inputDir,
        relativeFilePath = "**.csv"
      )

    val mateTransformed = V2FExtractionsAndTransforms
      .transform(MetaAnalysisTransEthnic)(mateExtractedAndConverted)

    // VariantEffectRegulatoryFeatureConsequences
    val verfcExtractedAndConverted =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectRegulatoryFeatureConsequences,
        pipelineContext,
        inputDir = inputDir,
        relativeFilePath = "*.csv"
      )

    val verfcTransformed =
      V2FExtractionsAndTransforms.transform(
        VariantEffectRegulatoryFeatureConsequences
      )(verfcExtractedAndConverted)

    // VariantEffectTranscriptConsequences
    val vetcExtractedAndConverted =
      V2FExtractionsAndTransforms.extractAndConvert(
        VariantEffectTranscriptConsequences,
        pipelineContext,
        inputDir = inputDir,
        relativeFilePath = "*.csv"
      )

    val vetcTransformed =
      V2FExtractionsAndTransforms.transform(
        VariantEffectTranscriptConsequences
      )(vetcExtractedAndConverted)

    // variant Msgs
    val faVariants =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        FrequencyAnalysis,
        faExtractedAndConverted
      )

    val mateVariants =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisTransEthnic,
        mateExtractedAndConverted
      )

    val maasVariants =
      V2FExtractionsAndTransforms.extractAndTransformVariants(
        MetaAnalysisAncestrySpecific,
        maasExtractedAndConverted
      )

    val variantMergedMsg =
      V2FExtractionsAndTransforms.mergeVariantMsgs(
        List(
          faVariants,
          maasVariants,
          mateVariants
        )
      )

    // save the extracted and transformed Msgs
    writeToDisk(
      faTransformed,
      FrequencyAnalysis.tableName,
      filePath = FrequencyAnalysis.filePath,
      outputDir
    )

    writeToDisk(
      vetcTransformed,
      VariantEffectTranscriptConsequences.tableName,
      filePath = VariantEffectTranscriptConsequences.filePath,
      outputDir
    )

    writeToDisk(
      maasTransformedAndAncestryID,
      MetaAnalysisAncestrySpecific.tableName,
      filePath = MetaAnalysisAncestrySpecific.filePath,
      outputDir
    )

    writeToDisk(
      mateTransformed,
      MetaAnalysisTransEthnic.tableName,
      filePath = MetaAnalysisTransEthnic.filePath,
      outputDir
    )

    writeToDisk(
      verfcTransformed,
      VariantEffectRegulatoryFeatureConsequences.tableName,
      filePath = VariantEffectRegulatoryFeatureConsequences.filePath,
      outputDir
    )

    MsgIO.writeJsonLists(
      variantMergedMsg,
      "Variants",
      s"${outputDir}/variants"
    )

    // waitUntilDone() throws error on failure
    pipelineContext //.waitUntilDone()
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
