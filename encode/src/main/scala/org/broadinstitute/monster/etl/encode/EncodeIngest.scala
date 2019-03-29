package org.broadinstitute.monster.etl.encode

import caseapp._
import com.spotify.scio.{BuildInfo => _, io => _, _}
import com.spotify.scio.extra.json._
import io.circe.JsonObject
import org.broadinstitute.monster.etl.BuildInfo

/** Main entry-point for the ENCODE ETL workflow. */
object EncodeIngest {

  @AppName("ENCODE Ingest")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.encode.EncodeIngest")
  case class Args(
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE experiments")
    experimentsJson: String,
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE libraries")
    librariesJson: String,
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE biosamples")
    samplesJson: String,
    @HelpMessage("Path to directory where ETL output JSON should be written")
    outputDir: String
  )

  def main(rawArgs: Array[String]): Unit = {
    // Using `typed` gives us '--help' and '--usage' automatically.
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // Declare source nodes in the workflow topology, reading in raw JSON.
    val rawExperiments = pipelineContext.jsonFile[JsonObject](parsedArgs.experimentsJson)
    val rawLibraries = pipelineContext.jsonFile[JsonObject](parsedArgs.librariesJson)
    val rawSamples = pipelineContext.jsonFile[JsonObject](parsedArgs.samplesJson)

    // Generate "cleaned" versions of each entity type, without joins.
    val cleanedExperiments =
      rawExperiments.transform(EncodeTransforms.cleanEntities(EncodeEntity.Experiment))
    val cleanedLibraries =
      rawLibraries.transform(EncodeTransforms.cleanEntities(EncodeEntity.Library))
    val cleanedSamples =
      rawSamples.transform(EncodeTransforms.cleanEntities(EncodeEntity.Sample))

    // TODO: Generate join tables & assay entities.

    // Write cleaned outputs back to disk.
    cleanedExperiments.saveAsJsonFile(s"${parsedArgs.outputDir}/experiments")
    cleanedLibraries.saveAsJsonFile(s"${parsedArgs.outputDir}/libraries")
    cleanedSamples.saveAsJsonFile(s"${parsedArgs.outputDir}/biosamples")

    pipelineContext.close().waitUntilDone()
    ()
  }

}
