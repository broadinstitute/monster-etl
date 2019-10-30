package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about an organization which has submitted findings to ClinVar.
  *
  * @param id ClinVar-assigned ID for the organization
  * @param submitterName full name of the organization
  * @param orgCategory category of the organization
  * @param orgAbbrev short name of the organization
  */
case class Submitter(
  id: String,
  submitterName: Option[String],
  orgCategory: Option[String],
  orgAbbrev: Option[String]
)

object Submitter {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Submitter] = deriveEncoder(renaming.snakeCase, None)

  /** Extract submitter-related info from a raw ClinicalAssertion payload. */
  def fromRawAssertion(rawAssertion: Msg): Submitter =
    Submitter(
      id = rawAssertion
        .extract("ClinVarAccession", "@OrgID")
        .getOrElse {
          throw new IllegalStateException(s"Found an SCV with no Org ID: $rawAssertion")
        }
        .str,
      submitterName =
        rawAssertion.extract("ClinVarAccession", "@SubmitterName").map(_.str),
      orgCategory =
        rawAssertion.extract("ClinVarAccession", "@OrganizationCategory").map(_.str),
      orgAbbrev = rawAssertion.extract("ClinVarAccession", "@OrgAbbreviation").map(_.str)
    )
}
