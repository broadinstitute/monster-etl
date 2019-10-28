package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/** TODO */
case class SCVTraitSet(
  id: String,
  clinicalAssertionId: Option[String],
  clinicalAssertionObservationId: Option[String],
  `type`: Option[String]
)

object SCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def fromRawAssertionSet(scv: SCV, rawSet: Msg): SCVTraitSet =
    fromRawSet(scv.id, Some(scv.id), None, rawSet)

  /** TODO */
  def fromRawObservationSet(observation: SCVObservation, rawSet: Msg): SCVTraitSet =
    fromRawSet(observation.id, None, Some(observation.id), rawSet)

  /** TODO */
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
