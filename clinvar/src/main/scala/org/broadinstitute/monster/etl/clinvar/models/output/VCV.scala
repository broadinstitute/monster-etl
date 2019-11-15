package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.{
  Interpretation,
  WithContent
}
import upack.Msg

/**
  * Top-level model linking a variation to ClinVar-specific provenance info.
  *
  * @param id ClinVar accession for the archive
  * @param version version of the archvie containing the remaining fields
  * @param variationId unique ID of the variation referenced by the archive
  * @param dateCreated the day the archive was created
  * @param dateLastUpdated the day the archive was last updated
  * @param numSubmissions number of submissions included in the archive
  * @param numSubmitters number of submitters contributing to the archive
  * @param recordStatus description of the archive's current state in ClinVar's database
  * @param reviewStatus description of the archive's current state in ClinVar's review process
  * @param species ID of the species referred to by the archive
  * @param interpDateLastEvaluated the date the interpretation was last evaluated at
  * @param interpType the type of interpretation, such as "clinical significance"
  * @param interpDescription a simple interpretation description, like "pathogenic"
  * @param interpExplanation a more detailed interpretation explanation than the description
  * @param interpContent the remaining content, if any, of the interpretation
  */
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
  species: Option[String],
  interpDateLastEvaluated: Option[String],
  interpType: Option[String],
  interpDescription: Option[String],
  interpExplanation: Option[String],
  interpContent: Option[String]
)

object VCV {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCV] = deriveEncoder(renaming.snakeCase, None)

  /** Extract VCV-related info from a raw VariationArchive payload. */
  def fromRawArchive(
    variation: Variation,
    interpWithContent: WithContent[Interpretation],
    rawArchive: Msg
  ): VCV =
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
      recordStatus = rawArchive.extract("RecordStatus").map(_.value.str),
      reviewStatus =
        rawArchive.extract("InterpretedRecord", "ReviewStatus").map(_.value.str),
      species = rawArchive.extract("Species").map(_.value.str),
      interpDateLastEvaluated = interpWithContent.data.dateLastEvaluated,
      interpType = interpWithContent.data.`type`,
      interpDescription = interpWithContent.data.description,
      interpExplanation = interpWithContent.data.explanation,
      interpContent = interpWithContent.content
    )
}
