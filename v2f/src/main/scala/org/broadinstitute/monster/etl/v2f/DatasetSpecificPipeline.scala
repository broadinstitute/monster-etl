package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.{ContextAndArgs, ScioContext, BuildInfo => _, io => _}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.etl._
import upack.Msg

/**
  * ETL workflow for converting and transforming the Dataset Specific Analysis from V2F.
  */
object DatasetSpecificPipeline {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("V2F Dataset Specific Pipeline")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.v2f.DatasetSpecificPipeline")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * Scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
                   @HelpMessage("Directory containing analysis files for V2F")
                   inputDir: String,
                   @HelpMessage("Path of directory where the processed Dataset Specific JSON should be written")
                   outputDir: String
                 )

  /**
    * Convert Dataset Specific JSON to Msg and performing transformations for ingest into the Data Repository.
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // extract and convert TSVs to Msg, transform Msg and then save Msg
    // FrequencyAnalysis
    convertAndWrite(pipelineContext, parsedArgs.inputDir, parsedArgs.outputDir).close()
    () // return Unit type
  }

  /**
    * Convert Dataset Specific JSON to Msg and perform necessary transformations.
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
      inputDir = inputDir
    )

    val faTransformed = V2FExtractionsAndTransforms
      .transform(FrequencyAnalysis)(faExtractedAndConverted)

    // save the extracted and transformed Msgs
    writeToDisk(
      faTransformed,
      FrequencyAnalysis.tableName,
      filePath = FrequencyAnalysis.filePath,
      outputDir
    )

    val dsaMessages = MsgIO.readJsonLists(
      pipelineContext,
      "Read Dataset Specific files",
      "**.json"
    )

    dsaTransformed = V2FExtractionsAndTransforms.transform(DatasetSpecificAnalysis)(dsaMessages)

    MsgIO.writeJsonLists(
      variantMergedMsg,
      "Variants",
      s"${outputDir}/variants"
    )

    pipelineContext
  }

  /**
    *  Write all the converted and transformed Msg Objects to disk.
    *
    * @param msgAndFilePaths the collection of Msg Objects and associated file paths that will be saved as a JSON file
    * @param filePath File pattern matching TSVs to process within the V2F analysis directory
    * @param outputDir the root outputs directory where the JSON file(s) will be saved
    */
  def writeToDisk(
                   msgAndFilePaths: SCollection[Msg],
                   description: String,
                   filePath: String,
                   outputDir: String
                 ): Unit = {
    MsgIO.writeJsonLists(
      msgAndFilePaths,
      description,
      s"$outputDir/$filePath"
    )
    ()
  }
}
