package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.WithContent
import upack.Msg

/**
  * Info about a collection of traits in ClinVar.
  *
  * @param id unique ID of the trait collection
  * @param `type` common type of the trait collection
  */
case class VCVTraitSet(id: String, `type`: Option[String], traitIds: Array[String])

object VCVTraitSet {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVTraitSet] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  def fromRawSet(rawSet: Msg, vcvTraits: Array[WithContent[VCVTrait]]): VCVTraitSet = VCVTraitSet(
    id = rawSet.extract("@ID").map(_.str).getOrElse {
      throw new IllegalStateException(s"Found a VCV Trait Set with no ID: $rawSet")
    },
    `type` = rawSet.extract("@Type").map(_.str),
    traitIds = vcvTraits.map(_.data.id)
  )
}
