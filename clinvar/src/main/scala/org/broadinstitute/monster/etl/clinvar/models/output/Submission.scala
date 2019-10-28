package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/** TODO */
case class Submission(
  id: String,
  submissionDate: String,
  submitterId: String,
  submissionNames: Array[String]
)

object Submission {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Submission] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def fromRawAssertion(submitter: Submitter, rawAssertion: Msg): Submission = {
    val submitDate = rawAssertion
      .extract("@SubmissionDate")
      .getOrElse {
        throw new IllegalStateException(
          s"Found an SCV with no submission date: $rawAssertion"
        )
      }
      .str

    Submission(
      id = s"${submitter.id}.$submitDate",
      submissionDate = submitDate,
      submitterId = submitter.id,
      submissionNames = rawAssertion
        .extractList("SubmissionNameList", "SubmissionName")
        .map(_.str)
        .toArray
    )
  }
}
