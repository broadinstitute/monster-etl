package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/** TODO */
case class VCV(
  id: String,
  version: Long,
  variationId: String,
  dateCreated: Option[String],
  dateLastUpdated: Option[String],
  numSubmissions: Option[Long],
  numSubmitters: Option[Long],
  recordStatus: Option[String],
  reviewStatus: Option[String],
  species: Option[String]
)

object VCV {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCV] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def fromRawArchive(variation: Variation, rawArchive: Msg): VCV =
    VCV(
      id = rawArchive
        .extract("@Accession")
        .getOrElse {
          throw new IllegalStateException(s"Found a VCV with no ID: $rawArchive")
        }
        .str,
      version = rawArchive
        .extract("@Version")
        .getOrElse {
          throw new IllegalStateException(s"Found a VCV with no version: $rawArchive")
        }
        .str
        .toLong,
      variationId = variation.id,
      dateCreated = rawArchive.extract("@DateCreated").map(_.str),
      dateLastUpdated = rawArchive.extract("@DateLastUpdated").map(_.str),
      numSubmissions = rawArchive.extract("@NumberOfSubmissions").map(_.str.toLong),
      numSubmitters = rawArchive.extract("@NumberOfSubmitters").map(_.str.toLong),
      recordStatus = rawArchive.extract("RecordStatus").map(_.str),
      reviewStatus = rawArchive.extract("InterpretedRecord", "ReviewStatus").map(_.str),
      species = rawArchive.extract("Species").map(_.str)
    )
}
