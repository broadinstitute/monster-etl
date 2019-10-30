package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import ujson.StringRenderer
import upack.{Arr, Msg, Obj, Str}

import scala.util.matching.Regex

/**
  * A submission to ClinVar about a single variation.
  *
  * @param id ClinVar accession for the submission
  * @param version version of the submission containing the remaining fields
  * @param variationId unique ID for the variation referred to / generated by
  *                    this submission
  * @param vcvId unique ID for the variation archive which captures the submission
  * @param submitterId unique ID of the organization which made the submission
  * @param submissionId unique ID of the submission batch which contained this submission
  * @param assertionType description of the clinical significance asserted by
  *                      this submission
  * @param dateCreated the day the submission was registered in ClinVar
  * @param dateLastUpdated the day when the last update to this submission
  *                        was registered in ClinVar
  * @param recordStatus description of the info's current state in ClinVar's database
  * @param reviewStatus description of the info's current state in ClinVar's
  *                     review process
  * @param title title for the submission
  * @param localKey identifier used by the submitter to refer to this submission
  * @param submittedAssembly genome assembly used to determine that the variation wrapped
  *                          in this submission actually exists
  * @param interpretationDescription clinical significance of the variation described by
  *                                  the submission, as seen by the submitter
  * @param interpretationLastEvaluated the day when the clinical significance of this
  *                                    submission was last updated
  * @param interpretationComments comments supporting the submitted clinical significance
  */
case class SCV(
  id: String,
  version: Long,
  variationId: String,
  vcvId: String,
  submitterId: String,
  submissionId: String,
  assertionType: Option[String],
  dateCreated: Option[String],
  dateLastUpdated: Option[String],
  recordStatus: Option[String],
  reviewStatus: Option[String],
  title: Option[String],
  localKey: Option[String],
  submittedAssembly: Option[String],
  interpretationDescription: Option[String],
  interpretationLastEvaluated: Option[String],
  interpretationComments: Array[String]
)

object SCV {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCV] = deriveEncoder(renaming.snakeCase, None)

  val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Parse a raw ClinicalAssertion payload into our expected model. */
  def fromRawAssertion(
    variation: Variation,
    vcv: VCV,
    submitter: Submitter,
    submission: Submission,
    rawAssertion: Msg
  ): SCV = SCV(
    id = rawAssertion
      .extract("ClinVarAccession", "@Accession")
      .getOrElse {
        throw new IllegalStateException(s"Found an SCV with no ID: $rawAssertion")
      }
      .str,
    version = rawAssertion
      .extract("ClinVarAccession", "@Version")
      .getOrElse {
        throw new IllegalStateException(s"Found an SCV with no version: $rawAssertion")
      }
      .str
      .toLong,
    variationId = variation.id,
    vcvId = vcv.id,
    submitterId = submitter.id,
    submissionId = submission.id,
    assertionType = rawAssertion.extract("Assertion").map(_.str),
    dateCreated = rawAssertion.extract("@DateCreated").map(_.str),
    dateLastUpdated = rawAssertion.extract("@DateLastUpdated").map(_.str),
    recordStatus = rawAssertion.extract("RecordStatus").map(_.str),
    reviewStatus = rawAssertion.extract("ReviewStatus").map(_.str),
    title = rawAssertion.extract("ClinVarSubmissionID", "@title").map(_.str),
    localKey = rawAssertion.extract("ClinVarSubmissionID", "@localKey").map(_.str),
    submittedAssembly =
      rawAssertion.extract("ClinVarSubmissionID", "@submittedAssembly").map(_.str),
    interpretationDescription =
      rawAssertion.extract("Interpretation", "Description").map(_.str),
    interpretationLastEvaluated = rawAssertion
      .extract("Interpretation", "@DateLastEvaluated")
      .flatMap(normalizeEvaluationDate),
    interpretationComments = rawAssertion
      .extract("Interpretation", "Comment")
      .fold(Array.empty[String])(normalizeComments)
  )

  /**
    * Regex matching the YYYY-MM-DD portion of a date field which might also contain
    * a trailing timestamp.
    *
    * Used to normalize fields which are intended to be dates, not timestamps.
    */
  val DatePattern: Regex = """^(\d{4}-\d{2}-\d{2}).*""".r

  /**
    * interp_date_last_evaluated *sometimes* contains hour values, which breaks BQ.
    * The hour values aren't really important, so we strip them out when present.
    */
  def normalizeEvaluationDate(rawDate: Msg): Option[String] = rawDate.str match {
    case DatePattern(trimmedDate) => Some(trimmedDate)
    case other =>
      logger.warn(s"Found un-parseable date [$other] in SCV")
      None
  }

  /**
    * SCV comments always contain a text body, and sometimes are tagged with a type.
    * If they have a type, they'll be extracted as objects.
    * Otherwise they'll be extracted as scalar strings.
    * We normalize them to always be stored as stringified JSON objects, with
    * an 'unknown' type when needed.
    */
  def normalizeComments(rawComments: Msg): Array[String] = {
    def normalizeComment(comment: Msg) =
      upack
        .transform(
          comment match {
            case Obj(fields) =>
              Obj(Str("type") -> fields(Str("@Type")), Str("text") -> fields(Str("$")))
            case other =>
              Obj(Str("type") -> Str("unknown"), Str("text") -> other)
          },
          StringRenderer()
        )
        .toString

    rawComments match {
      case Arr(comments) => comments.map(normalizeComment).toArray
      case comment       => Array(normalizeComment(comment))
    }
  }
}
