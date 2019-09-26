package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
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

    // Read the nested archives from storage.
    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${parsedArgs.inputPrefix}/VariationArchive/*.json"
      )

    // First split apart all of the entities that already exist in the archives.
    val (rawVcvs, rawRcvs, rawVariations, rawScvs, rawScvVariations) =
      ClinvarSplitters.splitArchives(fullArchives)

    // Map the fields and types of each entity stream.
    val vcvs = rawVcvs.transform("Cleanup VCVs")(_.map(ClinvarMappers.mapVcv))
    val rcvs = rawRcvs.transform("Cleanup RCVs")(_.map(ClinvarMappers.mapRcv))
    val variations = rawVariations.transform("Cleanup VCV Variations")(
      _.map(ClinvarMappers.mapVcvVariation)
    )
    val scvs = rawScvs.transform("Cleanup SCVs")(_.map(ClinvarMappers.mapScv))
    val scvVariations = rawScvVariations.transform("Cleanup SCV Variations")(
      _.map(ClinvarMappers.mapScvVariation)
    )

    // Further split the VCV stream to create a releases table.
    val (variationArchives, releases) = ClinvarSplitters.splitVcvs(vcvs)

    // Further split the SCV stream to create new submitter and submission entities.
    val (clinicalAssertions, submitters, submissions) = ClinvarSplitters.splitScvs(scvs)

    // Write everything back to storage.
    MsgIO.writeJsonLists(
      variationArchives,
      "VCVs",
      s"${parsedArgs.outputPrefix}/variation_archive"
    )
    MsgIO.writeJsonLists(
      releases,
      "VCV Releases",
      s"${parsedArgs.outputPrefix}/variation_archive_release"
    )
    MsgIO.writeJsonLists(
      rcvs,
      "RCV Accessions",
      s"${parsedArgs.outputPrefix}/rcv_accession"
    )
    MsgIO.writeJsonLists(
      variations,
      "Variations",
      s"${parsedArgs.outputPrefix}/variation"
    )
    MsgIO.writeJsonLists(
      clinicalAssertions,
      "SCVs",
      s"${parsedArgs.outputPrefix}/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      submitters,
      "Submitters",
      s"${parsedArgs.outputPrefix}/submitter"
    )
    MsgIO.writeJsonLists(
      submissions,
      "Submissions",
      s"${parsedArgs.outputPrefix}/submission"
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
