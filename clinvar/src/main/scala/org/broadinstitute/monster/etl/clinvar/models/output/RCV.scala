package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/** TODO */
case class RCV(
  id: String,
  version: Long,
  variationId: String,
  vcvId: String,
  title: Option[String],
  dateLastEvaluated: Option[String],
  reviewStatus: Option[String],
  interpretation: Option[String],
  submissionCount: Option[Long],
  independentObservations: Option[Long]
)

object RCV {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[RCV] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def fromRawAccession(variation: Variation, vcv: VCV, rawRcvAccession: Msg): RCV =
    RCV(
      id = rawRcvAccession
        .extract("@Accession")
        .getOrElse {
          throw new IllegalStateException(s"Found an RCV with no ID: $rawRcvAccession")
        }
        .str,
      version = rawRcvAccession
        .extract("@Version")
        .getOrElse {
          throw new IllegalStateException(
            s"Found an RCV with no version: $rawRcvAccession"
          )
        }
        .str
        .toLong,
      variationId = variation.id,
      vcvId = vcv.id,
      title = rawRcvAccession.extract("@Title").map(_.str),
      dateLastEvaluated = rawRcvAccession.extract("@DateLastEvaluated").map(_.str),
      reviewStatus = rawRcvAccession.extract("@ReviewStatus").map(_.str),
      interpretation = rawRcvAccession.extract("@Interpretation").map(_.str),
      submissionCount = rawRcvAccession.extract("@SubmissionCount").map(_.str.toLong),
      independentObservations =
        rawRcvAccession.extract("@independentObservations").map(_.str.toLong)
    )
}
