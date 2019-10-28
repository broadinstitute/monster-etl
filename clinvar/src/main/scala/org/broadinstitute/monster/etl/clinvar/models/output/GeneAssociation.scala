package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

case class GeneAssociation(
  geneId: String,
  variationId: String,
  relationshipType: Option[String],
  source: Option[String]
)

object GeneAssociation {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[GeneAssociation] = deriveEncoder(renaming.snakeCase, None)

  def fromRawGene(gene: Gene, variation: Variation, rawGene: Msg): GeneAssociation =
    GeneAssociation(
      geneId = gene.id,
      variationId = variation.id,
      relationshipType = rawGene.extract("@RelationshipType").map(_.str),
      source = rawGene.extract("@Source").map(_.str)
    )
}
