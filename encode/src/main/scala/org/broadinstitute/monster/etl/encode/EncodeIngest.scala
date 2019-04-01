package org.broadinstitute.monster.etl.encode

import caseapp._
import com.spotify.scio.{BuildInfo => _, io => _, _}
import com.spotify.scio.extra.json._
import io.circe.JsonObject
import org.broadinstitute.monster.etl.BuildInfo
import org.broadinstitute.monster.etl.encode.transforms.{
  DonorTransforms,
  EncodeTransforms,
  FileTransforms
}

/**
  * ETL workflow for cleaning ENCODE metadata pre-ingest.
  *
  * For now, requires a preceding step to download raw ENCODE JSON
  * into a location that Apache Beam can access (local storage or GCS).
  */
object EncodeIngest {

  @AppName("ENCODE Ingest")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.encode.EncodeIngest")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE donors")
    donorsJson: String,
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE experiments")
    experimentsJson: String,
    @HelpMessage("Path to newline-delimited JSON describing raw ENCODE files")
    filesJson: String,
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
    val rawDonors = pipelineContext.jsonFile[JsonObject](parsedArgs.donorsJson)
    val rawExperiments = pipelineContext.jsonFile[JsonObject](parsedArgs.experimentsJson)
    val rawFiles = pipelineContext.jsonFile[JsonObject](parsedArgs.filesJson)
    val rawLibraries = pipelineContext.jsonFile[JsonObject](parsedArgs.librariesJson)
    val rawSamples = pipelineContext.jsonFile[JsonObject](parsedArgs.samplesJson)

    // Generate "cleaned" versions of each entity type, without joins.
    val cleanedDonors =
      rawDonors
        .transform(EncodeTransforms.cleanEntities(EncodeEntity.Donor))
        .transform(DonorTransforms.splitDonorPhenotypes)

    val cleanedExperiments =
      rawExperiments.transform(EncodeTransforms.cleanEntities(EncodeEntity.Experiment))

    // TODO: Aggregate & add audit info
    val cleanedFiles =
      rawFiles
        .transform(EncodeTransforms.cleanEntities(EncodeEntity.File))
        .transform(FileTransforms.extractFileQc)
        .transform(FileTransforms.markFileRunType)

    val cleanedLibraries =
      rawLibraries.transform(EncodeTransforms.cleanEntities(EncodeEntity.Library))

    val cleanedSamples =
      rawSamples.transform(EncodeTransforms.cleanEntities(EncodeEntity.Biosample))

    // TODO: Generate join tables & assay entities.

    // Write cleaned outputs back to disk.
    cleanedDonors.saveAsJsonFile(s"${parsedArgs.outputDir}/donors")
    cleanedExperiments.saveAsJsonFile(s"${parsedArgs.outputDir}/experiments")
    cleanedFiles.saveAsJsonFile(s"${parsedArgs.outputDir}/files")
    cleanedLibraries.saveAsJsonFile(s"${parsedArgs.outputDir}/libraries")
    cleanedSamples.saveAsJsonFile(s"${parsedArgs.outputDir}/biosamples")

    pipelineContext.close().waitUntilDone()
    ()
  }

}
