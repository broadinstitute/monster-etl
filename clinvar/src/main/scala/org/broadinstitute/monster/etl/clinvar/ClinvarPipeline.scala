package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.coders.Coder
import org.broadinstitute.monster.ClinvarBuildInfo
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
    runPipeline(pipelineContext, parsedArgs.inputPrefix, parsedArgs.outputPrefix).run()
    ()
  }

  def runPipeline(
    pipelineContext: ScioContext,
    inputPrefix: String,
    outputPrefix: String
  ): ScioContext = {
    // Read the nested archives from storage.
    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"$inputPrefix/VariationArchive/*.json"
      )

    // First split apart all of the entities that already exist in the archives.
    // Since individual archives are self-contained, nearly all of the pipeline's
    // logic is done in this step.
    val archiveBranches = ArchiveBranches.fromArchiveStream(fullArchives)

    // Write everything back to storage.
    MsgIO.writeJsonLists(
      archiveBranches.variations,
      "Variations",
      s"$outputPrefix/variation"
    )
    MsgIO.writeJsonLists(
      archiveBranches.genes,
      "Genes",
      s"$outputPrefix/gene"
    )
    MsgIO.writeJsonLists(
      archiveBranches.geneAssociations,
      "Gene Associations",
      s"$outputPrefix/gene_association"
    )
    MsgIO.writeJsonLists(
      archiveBranches.vcvs,
      "VCVs",
      s"$outputPrefix/variation_archive"
    )
    MsgIO.writeJsonLists(
      archiveBranches.vcvReleases,
      "VCV Releases",
      s"$outputPrefix/variation_archive_release"
    )
    MsgIO.writeJsonLists(
      archiveBranches.rcvs,
      "RCV Accessions",
      s"$outputPrefix/rcv_accession"
    )
    MsgIO.writeJsonLists(
      archiveBranches.scvs,
      "SCVs",
      s"$outputPrefix/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      archiveBranches.submitters,
      "Submitters",
      s"$outputPrefix/submitter"
    )
    MsgIO.writeJsonLists(
      archiveBranches.submissions,
      "Submissions",
      s"$outputPrefix/submission"
    )
    MsgIO.writeJsonLists(
      archiveBranches.scvVariations,
      "SCV Variations",
      s"$outputPrefix/clinical_assertion_variation"
    )
    MsgIO.writeJsonLists(
      archiveBranches.scvObservations,
      "SCV Observations",
      s"$outputPrefix/clinical_assertion_observation"
    )
    MsgIO.writeJsonLists(
      archiveBranches.scvTraitSets,
      "SCV Trait Sets",
      s"$outputPrefix/clinical_assertion_trait_set"
    )
    MsgIO.writeJsonLists(
      archiveBranches.scvTraits,
      "SCV Traits",
      s"$outputPrefix/clinical_assertion_trait"
    )
    MsgIO.writeJsonLists(
      archiveBranches.vcvTraitSets,
      "VCV Trait Sets",
      s"$outputPrefix/trait_set"
    )
    MsgIO.writeJsonLists(
      archiveBranches.vcvTraits,
      "VCV Traits",
      s"$outputPrefix/trait"
    )
    MsgIO.writeJsonLists(
      archiveBranches.traitMappings,
      "Trait Mappings",
      s"$outputPrefix/trait_mapping"
    )

    pipelineContext
  }
}
