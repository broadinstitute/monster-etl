package org.broadinstitute.monster.etl.clinvar.models.intermediate

import java.util.concurrent.atomic.AtomicInteger

import org.broadinstitute.monster.etl.MsgTransformations
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import org.broadinstitute.monster.etl.clinvar.models.output._
import upack.{Msg, Str}

import scala.collection.mutable

/**
  * Wrapper for the fully-parsed contents of a single ClinVar VariationArchive.
  *
  * This representation flattens the hierarchy between models contained within
  * the archive. The parsing process must generate cross-links.
  *
  * @param variation        general info about the top-level variation described
  *                         by the archive
  * @param genes            genes associated with the archive's top-level variation.
  *                         NOT guaranteed to be de-duplicated
  * @param geneAssociations descriptions of how the genes in `genes` relate
  *                         to `variation`
  * @param vcv              info about how `variation` was submitted to ClinVar and reviewed
  * @param vcvRelease       info about the release history of `vcv`
  * @param rcvs             records describing ClinVar's aggregate knowledge of `variation`
  * @param scvs             records describing individual submissions to ClinVar
  *                         about `variation`
  * @param submitters       info about the organizations that submitted `scvs`.
  *                         NOT guaranteed to be de-duplicated
  * @param submissions      info about when the members of `submitters` made
  *                         submissions to ClinVar. NOT guaranteed to be de-duplicated
  * @param scvVariations    info about the variations that were submitted for
  *                         each item in `scvs`
  * @param scvObservations  info about the sampling process associated with
  *                         each item in `scvs`
  * @param scvTraitSets     info about collections of `scvTraits` which were submitted
  *                         as part of each item in `scvs`
  * @param scvTraits        info about traits that were submitted for each item in `scvs`
  * @param vcvTraitSets     info about collections of `vcvTraits`
  * @param vcvTraits        info about traits
  * @param traitMappings    info about how the members of `scvTraits` link to the
  *                         members of `vcvTraits`
  */
case class VariationArchive(
  variation: WithContent[Variation],
  genes: Array[Gene],
  geneAssociations: Array[WithContent[GeneAssociation]],
  vcv: Option[WithContent[VCV]],
  vcvRelease: Option[VCVRelease],
  rcvs: Array[WithContent[RCV]],
  scvs: Array[WithContent[SCV]],
  submitters: Array[Submitter],
  submissions: Array[Submission],
  scvVariations: Array[WithContent[SCVVariation]],
  scvObservations: Array[WithContent[SCVObservation]],
  scvTraitSets: Array[WithContent[SCVTraitSet]],
  scvTraits: Array[WithContent[SCVTrait]],
  vcvTraitSets: Array[WithContent[VCVTraitSet]],
  vcvTraits: Array[WithContent[VCVTrait]],
  traitMappings: Array[TraitMapping]
)

object VariationArchive {

  import org.broadinstitute.monster.etl.clinvar.MsgOps

  /** Type for "real" VCVs backed by submissions to ClinVar. */
  val InterpretedRecord: Msg = Str("InterpretedRecord")

  /**
    * Type for "fake" VCVs generated by ClinVar to model nested variations
    * which don't have their own top-level submissions.
    */
  val IncludedRecord: Msg = Str("IncludedRecord")

  /**
    * Convert a raw VariationArchive payload into our parsed form.
    *
    * This process assumes:
    *   1. The input payload was produced by running a ClinVar XML release
    * through Monster's XML->JSON conversion program
    *   2. Each VariationArchive is self-contained, and cross-links
    * can be fully constructed between all sub-models without
    * examining other archive instances
    */
  def fromRawArchive(rawArchive: Msg): VariationArchive = {

    /*
     * ClinVar publishes two types of "record"s:
     *   1. InterpretedRecords are generated for each variation that is sent
     *      to ClinVar as the top-level focus of a clinical assertion. They
     *      are reviewed by experts, and therefore have ClinVar-specific
     *      provenance info attached to them.
     *   2. IncludedRecords are generated for variations that have never been
     *      the focus of a clinical assertion, but have been mentioned as
     *      descendants by other assertions (i.e. a SimpleAllele in a Haplotype).
     *      They aren't reviewed by experts, and have no meaningful ClinVar-
     *      specific provenance info.
     *
     * We want to collect the variation and gene info described by both types
     * of records.
     */
    val variationRecord = rawArchive.obj
      .get(InterpretedRecord)
      .orElse(rawArchive.obj.get(IncludedRecord))
      .getOrElse {
        throw new IllegalStateException(s"Found an archive with no record: $rawArchive")
      }

    // Get the top-level variation.
    val (rawVariation, variationType) =
      ClinvarConstants.VariationTypes
        .foldLeft(Option.empty[(Msg, String)]) { (acc, subtype) =>
          acc.orElse(variationRecord.obj.remove(subtype).map(_ -> subtype.str))
        }
        .getOrElse {
          throw new IllegalStateException(
            s"Found an archive with no variation: $variationRecord"
          )
        }
    val variation = Variation.fromRawVariation(variationType, rawVariation)

    // Extract and map gene associations out of the variation before
    // we do the final mapping, so they don't end up in the variation's
    // unmodeled content.
    val (genes, geneAssociations) = rawVariation
      .extractList("GeneList", "Gene")
      .map { rawGene =>
        val gene = Gene.fromRawGene(rawGene)
        val geneAssociation = GeneAssociation.fromRawGene(gene, variation, rawGene)
        (gene, WithContent.attachContent(geneAssociation, rawGene))
      }
      .unzip

    val outputBase = VariationArchive(
      variation = WithContent.attachContent(variation, rawVariation),
      genes = genes.toArray,
      geneAssociations = geneAssociations.toArray,
      vcv = None,
      vcvRelease = None,
      rcvs = Array.empty,
      scvs = Array.empty,
      scvVariations = Array.empty,
      submitters = Array.empty,
      submissions = Array.empty,
      scvObservations = Array.empty,
      scvTraitSets = Array.empty,
      scvTraits = Array.empty,
      vcvTraitSets = Array.empty,
      vcvTraits = Array.empty,
      traitMappings = Array.empty
    )

    // Since IncludedRecords don't contain meaningful provenance, we only
    // bother to do further processing for InterpretedRecords.
    if (rawArchive.obj.contains(InterpretedRecord)) {

      // pull out Interpretation for use in multiple locations
      val rawInterpretation =
        variationRecord.extract("Interpretations", "Interpretation").getOrElse {
          throw new IllegalStateException(
            s"Found a VCV with no Interpretation: $variationRecord"
          )
        }

      // use an Interpretation object to utilize the existing WithContent functionality
      // before passing it along to the fromRawArchive method of VCV
      val interpretation = Interpretation.fromRawInterp(rawInterpretation)
      val interpretationWithContent =
        WithContent.attachContent(interpretation, rawInterpretation)

      // Pull out top-level info about the VCV.
      val vcv = VCV.fromRawArchive(variation, interpretationWithContent, rawArchive)
      val vcvRelease = VCVRelease.fromRawArchive(vcv, rawArchive)

      val vcvTraitSets = new mutable.ArrayBuffer[WithContent[VCVTraitSet]]()
      val vcvTraits = new mutable.ArrayBuffer[WithContent[VCVTrait]]()
      rawInterpretation.extractList("ConditionList", "TraitSet").foreach { rawTraitSet =>
        val currentVcvTraitIds = new mutable.ArrayBuffer[String]()
        MsgTransformations.popAsArray(rawTraitSet, "Trait").foreach { rawTrait =>
          val vcvTrait = VCVTrait.fromRawTrait(rawTrait)
          vcvTraits.append(WithContent.attachContent(vcvTrait, rawTrait))
          currentVcvTraitIds.append(vcvTrait.id)
        }
        val vcvTraitSet = VCVTraitSet.fromRawSet(rawTraitSet, currentVcvTraitIds.toArray)
        vcvTraitSets.append(WithContent.attachContent(vcvTraitSet, rawTraitSet))
      }

      val traitSetsWithoutContent = vcvTraitSets.map(_.data).toArray
      val traitsWithoutContent = vcvTraits.map(_.data).toArray

      // Pull out any RCVs, cross-linking to the relevant trait sets.
      val rcvs = variationRecord.extractList("RCVList", "RCVAccession").map { rawRcv =>
        val rcv = RCV.fromRawAccession(
          variation,
          vcv,
          traitSetsWithoutContent,
          traitsWithoutContent,
          rawRcv
        )
        WithContent.attachContent(rcv, rawRcv)
      }

      // Pull out SCV<->VCV trait mappings.
      val traitMappings = variationRecord
        .extractList("TraitMappingList", "TraitMapping")
        .map(TraitMapping.fromRawMapping)
        .toArray

      // Narrow the search space needed in future cross-linking by grouping
      // mappings by their SCV.
      //
      // NOTE: This grouping is done by numeric ID, not accession.
      // We have to post-process the trait mappings after looping through
      // the SCVs to fix up the references.
      val mappingsByScvId = traitMappings.groupBy(_.clinicalAssertionId)

      // Pull out any SCVs, and related info.
      val scvs = new mutable.ArrayBuffer[WithContent[SCV]]()
      val scvVariations = new mutable.ArrayBuffer[WithContent[SCVVariation]]()
      val submitters = new mutable.ArrayBuffer[Submitter]()
      val submissions = new mutable.ArrayBuffer[Submission]()
      val observations = new mutable.ArrayBuffer[WithContent[SCVObservation]]()
      val scvTraitSets = new mutable.ArrayBuffer[WithContent[SCVTraitSet]]()
      val scvTraits = new mutable.ArrayBuffer[WithContent[SCVTrait]]()

      val scvIdToAccession = new mutable.HashMap[String, String]()
      variationRecord.extractList("ClinicalAssertionList", "ClinicalAssertion").foreach {
        rawScv =>
          val submitter = Submitter.fromRawAssertion(rawScv)
          val submission = Submission.fromRawAssertion(submitter, rawScv)
          val scvAccessionId = SCV.extractAccessionId(rawScv)

          // Extract trait-related data from the SCV.
          // Traits and trait sets are nested under both the top-level SCV
          // and individual clinical observations.
          //
          // NOTE: Because trait mappings link to the numeric ID for each SCV,
          // but we use the accession as the PK, we need to do a little bit
          // of post-processing on the mappings.
          val scvId = SCV.extractNumericId(rawScv)
          scvIdToAccession.update(scvId, scvAccessionId)
          val relevantMappings = mappingsByScvId.getOrElse(scvId, Array.empty)

          val (directScvTraitSet, directScvTraits) = rawScv
            .extract("TraitSet")
            .map { rawTraitSet =>
              val traitCounter = new AtomicInteger(0)
              val currentScvTraits =
                MsgTransformations.popAsArray(rawTraitSet, "Trait").map { rawTrait =>
                  val scvTrait = SCVTrait.fromRawTrait(
                    // the setId is the same as the scv.id when extracting from assertions
                    scvAccessionId,
                    traitsWithoutContent,
                    relevantMappings,
                    traitCounter,
                    rawTrait
                  )
                  WithContent.attachContent(scvTrait, rawTrait)
                }
              val traitSet = SCVTraitSet.fromRawAssertionSet(
                scvAccessionId,
                rawTraitSet,
                currentScvTraits.map(_.data.id).toArray
              )
              (WithContent.attachContent(traitSet, rawTraitSet), currentScvTraits)
            }
            .fold(
              (Option.empty[WithContent[SCVTraitSet]], Array.empty[WithContent[SCVTrait]])
            ) {
              case (traitSet, traits) => (Some(traitSet), traits.toArray)
            }
          directScvTraitSet.foreach(traitSet => scvTraitSets.append(traitSet))
          scvTraits.appendAll(directScvTraits)

          val observationCounter = new AtomicInteger(0)
          rawScv.extractList("ObservedInList", "ObservedIn").foreach { rawObservation =>
            val observationId =
              s"${scvAccessionId}.${observationCounter.getAndIncrement()}"
            // pull the observation's trait set and trait ids out for multiple uses and for pattern consistency
            val (observationScvTraitSet, observationScvTraits) = rawObservation
              .extract("TraitSet")
              .map { rawTraitSet =>
                val traitCounter = new AtomicInteger(0)
                val currentScvTraits =
                  MsgTransformations.popAsArray(rawTraitSet, "Trait").map { rawTrait =>
                    val scvTrait = SCVTrait.fromRawTrait(
                      // the setId is the same as the observation.id when extracting from observations
                      observationId,
                      traitsWithoutContent,
                      relevantMappings,
                      traitCounter,
                      rawTrait
                    )
                    WithContent.attachContent(scvTrait, rawTrait)
                  }
                val traitSet = SCVTraitSet.fromRawObservationSet(
                  observationId,
                  rawTraitSet,
                  currentScvTraits.map(_.data.id).toArray
                )
                (WithContent.attachContent(traitSet, rawTraitSet), currentScvTraits)
              }
              .fold(
                (
                  Option.empty[WithContent[SCVTraitSet]],
                  Array.empty[WithContent[SCVTrait]]
                )
              ) {
                case (traitSet, traits) => (Some(traitSet), traits.toArray)
              }
            // use the extracted observation trait ids to create the observation
            val observation = SCVObservation(
              s"${scvAccessionId}.${observationCounter.getAndIncrement()}",
              observationScvTraits.map(_.data.id)
            )
            observations.append(WithContent.attachContent(observation, rawObservation))
            observationScvTraitSet.foreach(traitSet => scvTraitSets.append(traitSet))
            scvTraits.appendAll(observationScvTraits)
          }

          submitters.append(submitter)
          submissions.append(submission)

          // NOTE: It's important to attach content at the very end, to be sure everything
          // that can be modeled has already been popped out of the raw data.

          val scv = SCV.fromRawAssertion(
            variation,
            vcv,
            submitter,
            submission,
            rawScv,
            scvAccessionId,
            directScvTraitSet.map(_.data),
            directScvTraits.map(_.data),
            observations.map(_.data).toArray,
            vcvTraitSets.map(_.data).toArray,
            rcvs.map(_.data).toArray
          )

          // Extract variation-related data from the SCV.
          scvVariations.appendAll(SCVVariation.allFromRawAssertion(scv, rawScv))

          scvs.append(WithContent.attachContent(scv, rawScv))
      }

      // Swap SCV accessions for their numeric IDs so the FK in the mapping table
      // actually works.
      val mappingsWithAccessionLinks = traitMappings.map { rawMapping =>
        rawMapping.copy(
          clinicalAssertionId = scvIdToAccession(rawMapping.clinicalAssertionId)
        )
      }

      outputBase.copy(
        vcv = Some(WithContent.attachContent(vcv, rawArchive)),
        vcvRelease = Some(vcvRelease),
        rcvs = rcvs.toArray,
        scvs = scvs.toArray,
        scvVariations = scvVariations.toArray,
        submitters = submitters.toArray,
        submissions = submissions.toArray,
        scvObservations = observations.toArray,
        scvTraitSets = scvTraitSets.toArray,
        scvTraits = scvTraits.toArray,
        vcvTraitSets = vcvTraitSets.toArray,
        vcvTraits = vcvTraits.toArray,
        traitMappings = mappingsWithAccessionLinks
      )
    } else {
      outputBase
    }
  }
}
