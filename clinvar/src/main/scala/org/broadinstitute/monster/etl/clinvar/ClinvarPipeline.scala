package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import org.broadinstitute.monster.ClinvarBuildInfo
import org.broadinstitute.monster.etl.{MsgIO, MsgTransformations, UpackMsgCoder}
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

    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${parsedArgs.inputPrefix}/VariationArchive/*.json"
      )

    val (rawVcvs, rawRcvs, rawVariations, rawScvs, rawScvVariations) =
      ClinvarSplitters.splitArchives(fullArchives)

    val vcvs = rawVcvs.transform("Cleanup VCVs") {
      _.map { rawVcv =>
        MsgTransformations.parseLongs(
          Set("version", "num_submissions", "num_submitters")
        )(ClinvarMappers.mapVcv(rawVcv))
      }
    }

    val rcvs = rawRcvs.transform("Cleanup RCVs") {
      _.map { rawRcv =>
        MsgTransformations.parseLongs(
          Set("version", "submission_count", "independent_observations")
        )(ClinvarMappers.mapRcv(rawRcv))
      }
    }

    val variations = rawVariations.transform("Cleanup VCV Variations") {
      _.map { rawVariation =>
        MsgTransformations.parseLongs(Set("allele_id", "num_chromosomes", "num_copies")) {
          MsgTransformations.ensureArrays(Set("protein_change"))(
            ClinvarMappers.mapVariation(rawVariation)
          )
        }
      }
    }

    val scvs = rawScvs.transform("Cleanup SCVs") {
      _.map { rawScv =>
        MsgTransformations.ensureArrays(Set("submission_names"))(
          ClinvarMappers.mapScv(rawScv)
        )
      }
    }

    val scvVariations = rawScvVariations.transform("Cleanup SCV Variations") {
      _.map(ClinvarMappers.mapScvVariation)
    }

    val (clinicalAssertions, submitters, submissions) = ClinvarSplitters.splitScvs(scvs)

    MsgIO.writeJsonLists(
      vcvs,
      "VCV",
      s"${parsedArgs.outputPrefix}/variation_archive"
    )
    MsgIO.writeJsonLists(
      rcvs,
      "RCV Accession",
      s"${parsedArgs.outputPrefix}/rcv_accession"
    )
    MsgIO.writeJsonLists(
      variations,
      "Variation Archive Variation",
      s"${parsedArgs.outputPrefix}/variation_archive_variation"
    )
    MsgIO.writeJsonLists(
      clinicalAssertions,
      "SCV Clinical Assertion",
      s"${parsedArgs.outputPrefix}/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      submitters,
      "SCV Submitters",
      s"${parsedArgs.outputPrefix}/submitter"
    )
    MsgIO.writeJsonLists(
      submissions,
      "SCV Submissions",
      s"${parsedArgs.outputPrefix}/submission"
    )
    MsgIO.writeJsonLists(
      scvVariations,
      "SCV Clinical Assertion Variation",
      s"${parsedArgs.outputPrefix}/clinical_assertion_variation"
    )

    pipelineContext.close()
    ()
  }
}
