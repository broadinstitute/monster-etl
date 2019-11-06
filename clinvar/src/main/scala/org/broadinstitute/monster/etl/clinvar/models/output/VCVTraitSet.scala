package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.{Msg, Str}

/**
  * Info about a collection of traits included in a submission to ClinVar.
  *
  * @param id unique ID of the trait collection
  * @param `type` common type of the trait collection
  */
case class VCVTraitSet(id: String, `type`: Option[String])

object VCVTraitSet {

  implicit val encoder: Encoder[VCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  def fromRawSet(rawSet: Msg): VCVTraitSet = VCVTraitSet(
    id = rawSet.obj.get(Str("@ID")).map(_.str).get,
    `type` = rawSet.obj.get(Str("@Type")).map(_.str)
  )
}
