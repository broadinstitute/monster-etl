package org.broadinstitute.monster.etl.encode.extract

import org.broadinstitute.monster.etl.encode.transforms._
import caseapp.HelpMessage
import com.spotify.scio.ContextAndArgs
import org.broadinstitute.monster.etl.encode.EncodeEntity
import org.broadinstitute.monster.etl.encode.extract.client.EncodeClient
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import io.circe.syntax._

/**
  * Extract raw metadata from ENCODE
  */
object EncodeIngestExtract extends IOApp {

  /**
    * Arguments for the pipeline
    * @param assayTypes a list of strings containing the assay types for the experiments
    * @param outputDir where to output the raw metadata
    */
  case class Args(
    @HelpMessage("List of Assays")
    assayTypes: List[String],
    @HelpMessage("Output dir to JSON for ENCODE outputs")
    outputDir: String
  )

  /**
    * pulling raw metadata using the ENCODE search client API for the following specific entity types...
    * Experiments, Files, Audits, Replicates, Libraries, Samples and Donors
    */
  override def run(rawArgs: List[String]): IO[ExitCode] = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs.toArray)

    EncodeClient.resource.use { client =>
      // for each entity, get the params and the extract based off of those params
      val encodeExtractions = new EncodeExtractions(client)

      // make assayTypes in scollection
      val experimentsSearchParams = encodeExtractions.getExperimentSearchParams(
        EncodeEntity.Experiment.entryName
      )(pipelineContext.parallelize(parsedArgs.assayTypes))

      val experiments = encodeExtractions.extractSearchParams(
        EncodeEntity.Experiment.entryName,
        EncodeEntity.Experiment.encodeApiName
      )(experimentsSearchParams)

      experiments.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Experiment.entryName}.json"
      )

      val fileIDParams = encodeExtractions.getIDParams(
        EncodeEntity.File.entryName,
        referenceField = "files",
        manyReferences = true
      )(experiments)

      val filteredFiles = filterFiles(EncodeEntity.File.entryName)(
        encodeExtractions.extractIDParamEntities(
          EncodeEntity.File.entryName,
          EncodeEntity.File.encodeApiName
        )(fileIDParams)
      )

      filteredFiles.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.File.entryName}.json"
      )

      val auditIDParams = encodeExtractions.getIDParams(
        EncodeEntity.Audit.entryName,
        referenceField = "@id",
        manyReferences = false
      )(filteredFiles)

      val transformedAudits = transformAudits(EncodeEntity.Audit.entryName)(
        encodeExtractions.extractIDParamEntities(
          EncodeEntity.Audit.entryName,
          EncodeEntity.Audit.encodeApiName
        )(
          auditIDParams
        )
      )

      transformedAudits.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Audit.entryName}.json"
      )

      val replicateIDParams = encodeExtractions.getIDParams(
        EncodeEntity.Replicate.entryName,
        referenceField = "replicates",
        manyReferences = true
      )(experiments)

      val replicates = encodeExtractions.extractIDParamEntities(
        EncodeEntity.Replicate.entryName,
        EncodeEntity.Replicate.encodeApiName
      )(replicateIDParams)

      replicates.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Replicate.entryName}.json"
      )

      val libraryIDParams = encodeExtractions.getIDParams(
        EncodeEntity.Library.entryName,
        referenceField = "library",
        manyReferences = false
      )(replicates)

      val libraries = encodeExtractions.extractIDParamEntities(
        EncodeEntity.Library.entryName,
        EncodeEntity.Library.encodeApiName
      )(libraryIDParams)

      libraries.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Library.entryName}.json"
      )

      val sampleIDParams = encodeExtractions.getIDParams(
        EncodeEntity.Biosample.entryName,
        referenceField = "biosample",
        manyReferences = false
      )(libraries)

      val samples = encodeExtractions.extractIDParamEntities(
        EncodeEntity.Biosample.entryName,
        EncodeEntity.Biosample.encodeApiName
      )(sampleIDParams)

      samples.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}.json"
      )

      val donorIDParams = encodeExtractions.getIDParams(
        EncodeEntity.Donor.entryName,
        referenceField = "donor",
        manyReferences = false
      )(samples)

      val donors = encodeExtractions.extractIDParamEntities(
        EncodeEntity.Donor.entryName,
        EncodeEntity.Donor.encodeApiName
      )(donorIDParams)

      donors.saveAsJsonFile(
        s"${parsedArgs.outputDir}/${EncodeEntity.Donor.entryName}.json"
      )

      // waitUntilDone() throws error on failure
      IO(pipelineContext.close().waitUntilDone()).as(ExitCode.Success)
    }
  }

  /**
    * Filter the files to make sure they are not any restricted or unavailable files.
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    **/
  def filterFiles(
    entryName: String
  ): SCollection[JsonObject] => SCollection[JsonObject] =
    _.transform(s"Filter $entryName") {
      _.filter { jsonObj =>
        jsonObj("no_file_available").fold(true)(_.equals(false.asJson)) &&
        jsonObj("restricted").fold(true)(_.equals(false.asJson))
      }
    }

  /**
    * Retain the Encode ID field ("@id") and Encode Audit field, ("audit") for Audits.
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    **/
  def transformAudits(
    entryName: String
  ): SCollection[JsonObject] => SCollection[JsonObject] =
    _.transform(s"Transform $entryName") {
      _.map { jsonObj =>
        Set("@id", "audit").foldLeft(JsonObject.empty) { (acc, field) =>
          jsonObj(field).fold(acc)(acc.add(field, _))
        }
      }

    }
}
