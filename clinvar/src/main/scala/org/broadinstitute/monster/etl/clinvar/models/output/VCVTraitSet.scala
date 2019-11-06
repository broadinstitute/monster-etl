package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about a collection of traits included in a submission to ClinVar.
  *
  * @param id unique ID of the trait collection
  * @param `type` common type of the trait collection
  */
case class VCVTraitSet(
                        id: String,
                        `type`: Option[String]
                      )

object VCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under a ClinicalAssertion. */
  def fromRawAssertionSet(scv: SCV, rawSet: Msg): SCVTraitSet =
    fromRawSet(scv.id, Some(scv.id), None, rawSet)

  /** Extract a TraitSet model from a raw TraitSet payload which was nested under observation data. */
  def fromRawObservationSet(observation: SCVObservation, rawSet: Msg): SCVTraitSet =
    fromRawSet(observation.id, None, Some(observation.id), rawSet)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  private def fromRawSet(
                          id: String,
                          scvId: Option[String],
                          observationId: Option[String],
                          rawSet: Msg
                        ): SCVTraitSet = SCVTraitSet(
    id = id,
    clinicalAssertionId = scvId,
    clinicalAssertionObservationId = observationId,
    `type` = rawSet.extract("@Type").map(_.str)
  )
}
