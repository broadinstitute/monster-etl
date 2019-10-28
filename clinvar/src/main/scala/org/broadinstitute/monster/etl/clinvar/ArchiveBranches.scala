package org.broadinstitute.monster.etl.clinvar

import com.spotify.scio.values.{SCollection, SideOutput}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.{
  VariationArchive,
  WithContent
}
import org.broadinstitute.monster.etl.clinvar.models.output._
import upack.Msg

/**
  * Collection of data streams produced by the initial splitting
  * operation performed on raw VariationArchive entries.
  */
case class ArchiveBranches(
  variations: SCollection[WithContent[Variation]],
  genes: SCollection[Gene],
  geneAssociations: SCollection[WithContent[GeneAssociation]],
  vcvs: SCollection[WithContent[VCV]],
  vcvReleases: SCollection[VCVRelease],
  rcvs: SCollection[WithContent[RCV]],
  submitters: SCollection[Submitter],
  submissions: SCollection[Submission],
  scvs: SCollection[WithContent[SCV]],
  scvVariations: SCollection[WithContent[SCVVariation]],
  scvObservations: SCollection[WithContent[SCVObservation]],
  scvTraitSets: SCollection[WithContent[SCVTraitSet]],
  scvTraits: SCollection[WithContent[SCVTrait]]
)

object ArchiveBranches {

  /**
    * Split a stream of raw VariationArchive entries into multiple
    * streams of un-nested entities.
    *
    * Cross-linking between entities in the output streams occurs
    * prior to elements being pushed out of the split step.
    */
  def fromArchiveStream(archiveStream: SCollection[Msg]): ArchiveBranches = {

    val geneOut = SideOutput[Gene]
    val geneAssociationOut = SideOutput[WithContent[GeneAssociation]]
    val vcvOut = SideOutput[WithContent[VCV]]
    val vcvReleaseOut = SideOutput[VCVRelease]
    val rcvOut = SideOutput[WithContent[RCV]]
    val submitterOut = SideOutput[Submitter]
    val submissionOut = SideOutput[Submission]
    val scvOut = SideOutput[WithContent[SCV]]
    val scvVariationOut = SideOutput[WithContent[SCVVariation]]
    val scvObservationOut = SideOutput[WithContent[SCVObservation]]
    val scvTraitSetOut = SideOutput[WithContent[SCVTraitSet]]
    val scvTraitOut = SideOutput[WithContent[SCVTrait]]

    val (variationStream, sideCtx) = archiveStream
      .withSideOutputs(
        geneOut,
        geneAssociationOut,
        vcvOut,
        vcvReleaseOut,
        rcvOut,
        submitterOut,
        submissionOut,
        scvOut,
        scvVariationOut,
        scvObservationOut,
        scvTraitSetOut,
        scvTraitOut
      )
      .withName("Split Variation Archives")
      .map { (rawArchive, ctx) =>
        // Beam prohibits mutating inputs, so we have to copy the archive before
        // processing it.
        val archiveCopy = upack.copy(rawArchive)
        // Parse the raw archive into the structures we care about.
        val parsed = VariationArchive.fromRawArchive(archiveCopy)
        // Output all the things!
        parsed.genes.foreach(ctx.output(geneOut, _))
        parsed.geneAssociations.foreach(ctx.output(geneAssociationOut, _))
        parsed.vcv.foreach(ctx.output(vcvOut, _))
        parsed.vcvRelease.foreach(ctx.output(vcvReleaseOut, _))
        parsed.rcvs.foreach(ctx.output(rcvOut, _))
        parsed.submitters.foreach(ctx.output(submitterOut, _))
        parsed.submissions.foreach(ctx.output(submissionOut, _))
        parsed.scvs.foreach(ctx.output(scvOut, _))
        parsed.scvVariations.foreach(ctx.output(scvVariationOut, _))
        parsed.scvObservations.foreach(ctx.output(scvObservationOut, _))
        parsed.scvTraitSets.foreach(ctx.output(scvTraitSetOut, _))
        parsed.scvTraits.foreach(ctx.output(scvTraitOut, _))
        // Use variation as the main output because each archive contains
        // exactly one of them.
        parsed.variation
      }

    ArchiveBranches(
      variations = variationStream,
      genes = sideCtx(geneOut),
      geneAssociations = sideCtx(geneAssociationOut),
      vcvs = sideCtx(vcvOut),
      vcvReleases = sideCtx(vcvReleaseOut),
      rcvs = sideCtx(rcvOut),
      submitters = sideCtx(submitterOut),
      submissions = sideCtx(submissionOut),
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut),
      scvObservations = sideCtx(scvObservationOut),
      scvTraitSets = sideCtx(scvTraitSetOut),
      scvTraits = sideCtx(scvTraitOut)
    )
  }
}