package org.broadinstitute.monster.etl.clinvar.models.intermediate

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about an Interpretation, an intermediary object to attach content to for a VCV
  *
  * @param dateLastEvaluated the date it was last evaluated at
  * @param `type` the type of interpretation, such as "clinical significance"
  * @param description a simple description, like "pathogenic"
  * @param explanation a more detailed explanation than the description
  */
case class Interpretation(
  dateLastEvaluated: Option[String],
  `type`: Option[String],
  description: Option[String],
  explanation: Option[String]
)

object Interpretation {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Interpretation] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a TraitSet model from a raw TraitSet payload. */
  def fromRawInterp(rawInterp: Msg): Interpretation =
    Interpretation(
      dateLastEvaluated = rawInterp.extract("@DateLastEvaluated").map(_.str),
      `type` = rawInterp.extract("@Type").map(_.str),
      description = rawInterp.extract("Description").map(_.value.str),
      explanation = rawInterp.extract("Explanation").map(_.value.str)
    )
}
