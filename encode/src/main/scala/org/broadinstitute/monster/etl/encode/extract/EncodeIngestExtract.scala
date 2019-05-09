package org.broadinstitute.monster.etl.encode.extract

import org.broadinstitute.monster.etl.encode.transforms._
import caseapp.HelpMessage
import com.spotify.scio.ContextAndArgs
import org.broadinstitute.monster.etl.encode.EncodeEntity
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import io.circe.syntax._

/**
  * Extract raw metadata from ENCODE
  */
object EncodeIngestExtract {

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
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs.toArray)

    // for each entity, get the params and the extract based off of those params

    // make assayTypes in scollection
    val experimentsSearchParams = EncodeExtractions.getExperimentSearchParams(
      EncodeEntity.Experiment.entryName
    )(pipelineContext.parallelize(parsedArgs.assayTypes))

    val experiments = EncodeExtractions.extractSearchParams(
      EncodeEntity.Experiment.entryName,
      EncodeEntity.Experiment.encodeApiName
    )(experimentsSearchParams)

    experiments.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Experiment.entryName}.json"
    )

    val fileIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.File.entryName,
      referenceField = "files",
      manyReferences = true
    )(experiments)

    val filteredFiles = filterFiles(EncodeEntity.File.entryName)(
      EncodeExtractions.extractIDParamEntities(
        EncodeEntity.File.entryName,
        EncodeEntity.File.encodeApiName
      )(fileIDParams)
    )

    filteredFiles.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.File.entryName}.json"
    )

    val auditIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.Audit.entryName,
      referenceField = "@id",
      manyReferences = false
    )(filteredFiles)

    val transformedAudits = transformAudits(EncodeEntity.Audit.entryName)(
      EncodeExtractions.extractIDParamEntities(
        EncodeEntity.Audit.entryName,
        EncodeEntity.Audit.encodeApiName
      )(
        auditIDParams
      )
    )

    transformedAudits.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Audit.entryName}.json"
    )

    val replicateIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.Replicate.entryName,
      referenceField = "replicates",
      manyReferences = true
    )(experiments)

    val replicates = EncodeExtractions.extractIDParamEntities(
      EncodeEntity.Replicate.entryName,
      EncodeEntity.Replicate.encodeApiName
    )(replicateIDParams)

    replicates.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Replicate.entryName}.json"
    )

    val libraryIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.Library.entryName,
      referenceField = "library",
      manyReferences = false
    )(replicates)

    val libraries = EncodeExtractions.extractIDParamEntities(
      EncodeEntity.Library.entryName,
      EncodeEntity.Library.encodeApiName
    )(libraryIDParams)

    libraries.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Library.entryName}.json"
    )

    val sampleIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.Biosample.entryName,
      referenceField = "biosample",
      manyReferences = false
    )(libraries)

    val samples = EncodeExtractions.extractIDParamEntities(
      EncodeEntity.Biosample.entryName,
      EncodeEntity.Biosample.encodeApiName
    )(sampleIDParams)

    samples.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}.json"
    )

    val donorIDParams = EncodeExtractions.getIDParams(
      EncodeEntity.Donor.entryName,
      referenceField = "donor",
      manyReferences = false
    )(samples)

    val donors = EncodeExtractions.extractIDParamEntities(
      EncodeEntity.Donor.entryName,
      EncodeEntity.Donor.encodeApiName
    )(donorIDParams)

    donors.saveAsJsonFile(
      s"${parsedArgs.outputDir}/${EncodeEntity.Donor.entryName}.json"
    )

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
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
