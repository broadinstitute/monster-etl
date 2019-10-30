package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Description of how a gene is associated with a variation.
  *
  * @param geneId unique ID for the gene
  * @param variationId unique ID for the variation
  * @param relationshipType description of how the gene and
  *                         variation are associated
  * @param source description of how the association was registered
  */
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
