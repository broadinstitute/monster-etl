package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import org.broadinstitute.monster.ClinvarBuildInfo
import org.broadinstitute.monster.etl.clinvar.splitters._
import org.broadinstitute.monster.etl.{MsgIO, UpackMsgCoder}
import upack._

object ClinvarPipeline {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("ClinVar transformation pipeline")
  @AppVersion(ClinvarBuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
  case class Args(
    @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
    inputPrefix: String,
    @HelpMessage("Path where transformed ClinVar JSON should be written")
    outputPrefix: String
  )

  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    // Read the nested archives from storage.
    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${parsedArgs.inputPrefix}/VariationArchive/*.json"
      )

    // First split apart all of the entities that already exist in the archives.
    val archiveBranches = ArchiveBranches.fromArchiveStream(fullArchives)

    // Map the fields and types of each entity stream.
    val variations = archiveBranches.variations.transform("Cleanup Variations")(
      _.map(ClinvarMappers.mapVariation)
    )
    val genes =
      archiveBranches.genes.transform("Cleanup Genes")(_.map(ClinvarMappers.mapGene))
    val vcvs =
      archiveBranches.vcvs.transform("Cleanup VCVs")(_.map(ClinvarMappers.mapVcv))
    val rcvs =
      archiveBranches.rcvs.transform("Cleanup RCVs")(_.map(ClinvarMappers.mapRcv))
    val scvs =
      archiveBranches.scvs.transform("Cleanup SCVs")(_.map(ClinvarMappers.mapScv))
    val scvObservations =
      archiveBranches.scvObservations.transform("Cleanup SCV Observations")(
        _.map(ClinvarMappers.mapScvObservation)
      )
    val scvVariations = archiveBranches.scvVariations.transform("Cleanup SCV Variations")(
      _.map(ClinvarMappers.mapScvVariation)
    )

    // Further split the gene stream to distinguish base genes from associations.
    val geneBranches = GeneBranches.fromGeneStream(genes)
    // Further split the VCV stream to create a releases table.
    val vcvBranches = VcvBranches.fromVcvStream(vcvs)
    // Further split the SCV stream to create new submitter and submission entities.
    val scvBranches = ScvBranches.fromScvStream(scvs)

    // Write everything back to storage.
    MsgIO.writeJsonLists(
      variations,
      "Variations",
      s"${parsedArgs.outputPrefix}/variation"
    )
    MsgIO.writeJsonLists(
      geneBranches.genes,
      "Genes",
      s"${parsedArgs.outputPrefix}/gene"
    )
    MsgIO.writeJsonLists(
      geneBranches.geneAssociations,
      "Gene Associations",
      s"${parsedArgs.outputPrefix}/gene_association"
    )
    MsgIO.writeJsonLists(
      vcvBranches.vcvs,
      "VCVs",
      s"${parsedArgs.outputPrefix}/variation_archive"
    )
    MsgIO.writeJsonLists(
      vcvBranches.vcvReleases,
      "VCV Releases",
      s"${parsedArgs.outputPrefix}/variation_archive_release"
    )
    MsgIO.writeJsonLists(
      rcvs,
      "RCV Accessions",
      s"${parsedArgs.outputPrefix}/rcv_accession"
    )
    MsgIO.writeJsonLists(
      scvBranches.scvs,
      "SCVs",
      s"${parsedArgs.outputPrefix}/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      scvBranches.submitters,
      "Submitters",
      s"${parsedArgs.outputPrefix}/submitter"
    )
    MsgIO.writeJsonLists(
      scvBranches.submissions,
      "Submissions",
      s"${parsedArgs.outputPrefix}/submission"
    )
    MsgIO.writeJsonLists(
      scvObservations,
      "SCV Observations",
      s"${parsedArgs.outputPrefix}/clinical_assertion_observation"
    )
    MsgIO.writeJsonLists(
      scvVariations,
      "SCV Variations",
      s"${parsedArgs.outputPrefix}/clinical_assertion_variation"
    )

    pipelineContext.close()
    ()
  }
}
