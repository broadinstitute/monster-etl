package org.broadinstitute.monster.etl.v2f

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.{ContextAndArgs, ScioContext, io => _}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.V2fBuildInfo
import org.broadinstitute.monster.etl._
import upack.Msg

/**
  * ETL workflow for converting and transforming the Dataset Specific Analysis from V2F.
  */
object DatasetSpecificPipeline {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("V2F Dataset Specific Pipeline")
  @AppVersion(V2fBuildInfo.version)
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
    @HelpMessage(
      "Path of directory where the processed Dataset Specific JSON should be written"
    )
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
    * @param inputDir        the directory from which to read data.
    * @param outputDir       the directory in which to write data.
    */
  def convertAndWrite(
    pipelineContext: ScioContext,
    inputDir: String,
    outputDir: String
  ): ScioContext = {

    val dsaMessages = MsgIO.readJsonLists(
      pipelineContext,
      "Read Dataset Specific files",
      s"$inputDir/${DatasetSpecificAnalysis.filePath}/**"
    )

    val dsaTransformed = transform(DatasetSpecificAnalysis)(dsaMessages)

    MsgIO.writeJsonLists(
      dsaTransformed,
      "Dataset Specific Analysis",
      s"$outputDir/${DatasetSpecificAnalysis.filePath}"
    )

    pipelineContext
  }

  /**
    * Given conversion functions, for the field names specified, the fields of a provided JSON Object are converted based on the given functions.
    *
    * @param v2fConstant the type that will be transformed
    */
  def transform(v2fConstant: V2FConstants): SCollection[Msg] => SCollection[Msg] = {
    msgs =>
      msgs.map { msg =>
        // snake case fields
        val withSnakeCase = MsgTransformations.keysToSnakeCase(msg)
        // rename fields
        val withRenamedFields =
          MsgTransformations.renameFields(v2fConstant.fieldsToRename)(withSnakeCase)
        // need to remove fields
        val withRemovedFields =
          MsgTransformations.removeFields(v2fConstant.fieldsToRemove)(withRenamedFields)
        // return final Msg
        withRemovedFields
      }
  }
}
