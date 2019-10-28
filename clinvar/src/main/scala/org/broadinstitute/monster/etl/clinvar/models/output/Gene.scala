package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

case class Gene(
  id: String,
  symbol: Option[String],
  hgncId: Option[String],
  fullName: Option[String]
)

object Gene {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Gene] = deriveEncoder(renaming.snakeCase, None)

  def fromRawGene(rawGene: Msg): Gene =
    Gene(
      id = rawGene
        .extract("@GeneID")
        .getOrElse {
          throw new IllegalStateException(s"Found a gene with no ID: $rawGene")
        }
        .str,
      symbol = rawGene.extract("@Symbol").map(_.str),
      hgncId = rawGene.extract("@HGNC_ID").map(_.str),
      fullName = rawGene.extract("FullName").map(_.str)
    )
}
