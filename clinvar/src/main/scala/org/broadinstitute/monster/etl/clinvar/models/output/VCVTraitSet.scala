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
case class VCVTraitSet(id: String, `type`: String)

object VCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  def fromRawSet(rawSet: Msg): VCVTraitSet = VCVTraitSet(
    id = rawSet.extract("@ID").map(_.str).get,
    `type` = rawSet.extract("@Type").map(_.str).get
  )
}
