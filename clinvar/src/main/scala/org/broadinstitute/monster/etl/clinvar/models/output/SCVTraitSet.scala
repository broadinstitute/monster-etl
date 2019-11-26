package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about a collection of traits included in a submission to ClinVar.
  *
  * @param id unique ID of the trait collection
  * @param clinicalAssertionId accession of the SCV which includes this set,
  *                            if the set was packaged directly under the SCV
  * @param clinicalAssertionObservationId unique ID of the observation data which
  *                                       includes this set, if the set was packaged
  *                                       directly under the observation
  * @param `type` common type of the trait collection
  * @param traitIds the IDs of the SCVTraits that make up the Trait Set
  */
case class SCVTraitSet(
  id: String,
  clinicalAssertionId: Option[String],
  clinicalAssertionObservationId: Option[String],
  `type`: Option[String],
  traitIds: Array[String]
)

object SCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under a ClinicalAssertion. */
  def fromRawAssertionSet(
    scvAccessionId: String,
    rawSet: Msg,
    traitIds: Array[String]
  ): SCVTraitSet =
    fromRawSet(scvAccessionId, Some(scvAccessionId), None, rawSet, traitIds)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under observation data. */
  def fromRawObservationSet(
    observation: SCVObservation,
    rawSet: Msg,
    traitIds: Array[String]
  ): SCVTraitSet =
    fromRawSet(observation.id, None, Some(observation.id), rawSet, traitIds)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  private def fromRawSet(
    id: String,
    scvId: Option[String],
    observationId: Option[String],
    rawSet: Msg,
    traitIds: Array[String]
  ): SCVTraitSet = SCVTraitSet(
    id = id,
    clinicalAssertionId = scvId,
    clinicalAssertionObservationId = observationId,
    `type` = rawSet.extract("@Type").map(_.str),
    traitIds = if (traitIds.isEmpty) {
      throw new IllegalStateException(s"No trait Ids found in TraitSet with id $id")
    } else {
      traitIds
    }
  )
}
