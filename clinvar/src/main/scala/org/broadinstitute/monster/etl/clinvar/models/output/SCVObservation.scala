package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}

/**
  * Data observed alongside the variations and traits of a submission to ClinVar.
  *
  * NOTE: Observed data is complex, so we're not modeling it for now.
  * This class is basically serving as a join-table.
  *
  * @param id unique ID of the observation
  * @param clinicalAssertionTraitIds The IDs of the traits in the observation
  */
case class SCVObservation(
  id: String,
  clinicalAssertionTraitIds: Array[String]
)

object SCVObservation {
  implicit val encoder: Encoder[SCVObservation] = deriveEncoder(renaming.snakeCase, None)
}
