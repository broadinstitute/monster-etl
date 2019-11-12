package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Description of a Gene which is associated with a variation
  * known to ClinVar.
  *
  * @param id unique ID assigned to the gene by ClinVar
  * @param symbol common short-hand ID associated with the gene (i.e. BRCA1)
  * @param hgncId ID of the gene in the HUGO reference DB
  * @param fullName long-form name for the gene
  */
case class Gene(
  id: String,
  symbol: Option[String],
  hgncId: Option[String],
  fullName: Option[String]
)

object Gene {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Gene] = deriveEncoder(renaming.snakeCase, None)

  /** Parse a raw Gene payload into our expected model. */
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
      fullName = rawGene.extract("FullName").map(_.value.str)
    )
}
