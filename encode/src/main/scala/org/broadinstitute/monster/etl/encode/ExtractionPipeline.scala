package org.broadinstitute.monster.etl.encode

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.coders.Coder
import com.spotify.scio.{BuildInfo => _, io => _, _}
import com.spotify.scio.extra.json._
import io.circe.JsonObject
import org.broadinstitute.monster.etl._
import org.broadinstitute.monster.etl.encode.extract.{
  AuditExtractions,
  EncodeExtractions,
  FileExtractions
}

/**
  * ETL workflow for scraping the latest entity metadata from ENCODE.
  */
object ExtractionPipeline {

  implicit val jsonCoder: Coder[JsonObject] = Coder.kryo[JsonObject]

  val DefaultAssays = List(
    "ATAC-seq",
    "ChIA-PET",
    "ChIP-seq",
    "DNase-seq",
    "Hi-C",
    "microRNA-seq",
    "polyA RNA-seq",
    "RRBS",
    "total RNA-seq",
    "WGBS"
  )

  @AppName("ENCODE Extraction Pipeline")
  @AppVersion(BuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.encode.ExtractionPipeline")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
    @HelpMessage(
      "Assay types (i.e. ChIP-seq) which should have all associated metadata extracted."
    )
    assayTypes: List[String] = DefaultAssays,
    @HelpMessage(
      "Path to directory where the extracted raw ENCODE metadata should be written"
    )
    outputDir: String
  )

  /**
    * pulling raw metadata using the ENCODE search client API for the following specific entity types...
    * Experiments, Files, Audits, Replicates, Libraries, Samples and Donors
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // Begin by pulling all experiment data for the set of assay types given as input.
    val experiments = {
      val assayTypes = pipelineContext.parallelize(parsedArgs.assayTypes).map { typ =>
        List("assay_title" -> typ)
      }

      EncodeExtractions.getEntities(EncodeEntity.Experiment)(assayTypes)
    }

    // Walk the metadata tree from the collected experiments to get the rest of the entities.
    val files = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.File.entryName,
        referenceField = "files",
        manyReferences = true
      )(experiments)

      val allFiles = EncodeExtractions.getEntitiesById(EncodeEntity.File)(idParams)

      FileExtractions.filterFiles(allFiles)
    }
    val audits = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Audit.entryName,
        referenceField = "@id",
        manyReferences = false
      )(files)

      val wrappedAudits = EncodeExtractions.getEntitiesById(EncodeEntity.Audit)(idParams)

      AuditExtractions.transformAudits(wrappedAudits)
    }
    val replicates = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Replicate.entryName,
        referenceField = "replicates",
        manyReferences = true
      )(experiments)

      EncodeExtractions.getEntitiesById(EncodeEntity.Replicate)(idParams)
    }
    val libraries = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Library.entryName,
        referenceField = "library",
        manyReferences = false
      )(replicates)

      EncodeExtractions.getEntitiesById(EncodeEntity.Library)(idParams)
    }
    val samples = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Biosample.entryName,
        referenceField = "biosample",
        manyReferences = false
      )(libraries)

      EncodeExtractions.getEntitiesById(EncodeEntity.Biosample)(idParams)
    }
    val donors = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Donor.entryName,
        referenceField = "donor",
        manyReferences = false
      )(samples)

      EncodeExtractions.getEntitiesById(EncodeEntity.Donor)(idParams)
    }

    // Write all the extracted JSON to disk.
    experiments.saveAsJsonFile(s"${parsedArgs.outputDir}/experiments")
    files.saveAsJsonFile(s"${parsedArgs.outputDir}/files")
    audits.saveAsJsonFile(s"${parsedArgs.outputDir}/audits")
    replicates.saveAsJsonFile(s"${parsedArgs.outputDir}/replicates")
    libraries.saveAsJsonFile(s"${parsedArgs.outputDir}/libraries")
    samples.saveAsJsonFile(s"${parsedArgs.outputDir}/biosamples")
    donors.saveAsJsonFile(s"${parsedArgs.outputDir}/donors")

    // waitUntilDone() throws error on failure
    pipelineContext.close().waitUntilDone()
    ()
  }
}
