package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}

/** TODO */
case class SCVObservation(
  id: String,
  clinicalAssertionId: String
)

object SCVObservation {
  implicit val encoder: Encoder[SCVObservation] = deriveEncoder(renaming.snakeCase, None)
}
