package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about a collection of traits included in a submission to ClinVar.
  *
  * @param id unique ID of the trait collection
  * @param `type` common type of the trait collection
  * @param clinicalAssertionTraitIds the IDs of the SCVTraits that make up the Trait Set
  */
case class SCVTraitSet(
  id: String,
  `type`: Option[String],
  clinicalAssertionTraitIds: Array[String]
)

object SCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under a ClinicalAssertion. */
  def fromRawAssertionSet(
    scvAccessionId: String,
    rawSet: Msg,
    scvTraitIds: Array[String]
  ): SCVTraitSet =
    fromRawSet(scvAccessionId, rawSet, scvTraitIds)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under observation data. */
  def fromRawObservationSet(
    observation: SCVObservation,
    rawSet: Msg,
    scvTraitIds: Array[String]
  ): SCVTraitSet =
    fromRawSet(observation.id, rawSet, scvTraitIds)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  private def fromRawSet(
    id: String,
    rawSet: Msg,
    scvTraitIds: Array[String]
  ): SCVTraitSet = SCVTraitSet(
    id = id,
    `type` = rawSet.extract("@Type").map(_.str),
    clinicalAssertionTraitIds = scvTraitIds
  )
}
