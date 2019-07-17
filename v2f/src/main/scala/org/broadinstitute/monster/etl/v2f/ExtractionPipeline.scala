package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{BuildInfo => _, io => _}
import io.circe.JsonObject
import org.broadinstitute.monster.etl._
import com.spotify.scio.extra.json._

/**
  * ETL workflow for converting and transforming TSVs from V2F.
  */
object ExtractionPipeline {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]

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

    // variant JSONs
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
      V2FExtractionsAndTransforms.mergeVariantJsons(
        List(
          frequencyAnalysisVariantJsonAndFilePaths,
          metaAnalysisAncestrySpecificVariantJsonAndFilePaths,
          metaAnalysisTransEthnicVariantJsonAndFilePaths
        )
      )

    // save the extracted and transformed JSONs
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
    *  Write all the converted and transformed JSON Objects to disk.
    *
    * @param jsonAndFilePaths the collection of JSON Objects and associated file paths that will be saved as a JSON file
    * @param filePath File pattern matching TSVs to process within the V2F analysis directory
    * @param outputDir the root outputs directory where the JSON file(s) will be saved
    */
  def writeToDisk(
    jsonAndFilePaths: SCollection[(String, JsonObject)],
    filePath: String,
    outputDir: String
  ): Unit = {
    jsonAndFilePaths.map {
      case (_, jsonObj) =>
        jsonObj
    }.saveAsJsonFile(
      s"$outputDir/$filePath"
    )
    ()
  }
}
