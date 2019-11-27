package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.WithContent
import ujson.StringRenderer
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * A submission to ClinVar about a single variation.
  *
  * @param id                          ClinVar accession for the submission
  * @param version                     version of the submission containing the remaining fields
  * @param variationId                 unique ID for the variation referred to / generated by
  *                                    this submission
  * @param vcvId                       unique ID for the variation archive which captures the submission
  * @param submitterId                 unique ID of the organization which made the submission
  * @param submissionId                unique ID of the submission batch which contained this submission
  * @param assertionType               description of the clinical significance asserted by
  *                                    this submission
  * @param dateCreated                 the day the submission was registered in ClinVar
  * @param dateLastUpdated             the day when the last update to this submission
  *                                    was registered in ClinVar
  * @param recordStatus                description of the info's current state in ClinVar's database
  * @param reviewStatus                description of the info's current state in ClinVar's
  *                                    review process
  * @param title                       title for the submission
  * @param localKey                    identifier used by the submitter to refer to this submission
  * @param submittedAssembly           genome assembly used to determine that the variation wrapped
  *                                    in this submission actually exists
  * @param interpretationDescription   clinical significance of the variation described by
  *                                    the submission, as seen by the submitter
  * @param interpretationLastEvaluated the day when the clinical significance of this
  *                                    submission was last updated
  * @param interpretationComments      comments supporting the submitted clinical significance
  * @param clinicalAssertionTraitSetId      the ID of the SCV Trait Set associated with this SCV
  * @param clinicalAssertionObservationIds      the IDs of the observations associated with this SCV
  * @param traitSetId               the ID of the associated vcv trait set, if there is one
  * @param rcvAccessionId                       The ID of the RCV that this SCV is related to
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
  interpretationComments: Array[String],
  clinicalAssertionTraitSetId: Option[String],
  clinicalAssertionObservationIds: Array[String],
  traitSetId: Option[String],
  rcvAccessionId: Option[String]
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
    rawAssertion: Msg,
    scvAccessionId: String,
    clinicalAssertionTraitSetId: Option[String],
    clinicalAssertionObservationIds: Array[String],
    vcvTraitSetId: Option[String],
    rcvId: Option[String]
  ): SCV = SCV(
    id = scvAccessionId,
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
    assertionType = rawAssertion.extract("Assertion").map(_.value.str),
    dateCreated = rawAssertion.extract("@DateCreated").map(_.str),
    dateLastUpdated = rawAssertion.extract("@DateLastUpdated").map(_.str),
    recordStatus = rawAssertion.extract("RecordStatus").map(_.value.str),
    reviewStatus = rawAssertion.extract("ReviewStatus").map(_.value.str),
    title = rawAssertion.extract("ClinVarSubmissionID", "@title").map(_.str),
    localKey = rawAssertion.extract("ClinVarSubmissionID", "@localKey").map(_.str),
    submittedAssembly =
      rawAssertion.extract("ClinVarSubmissionID", "@submittedAssembly").map(_.str),
    interpretationDescription =
      rawAssertion.extract("Interpretation", "Description").map(_.value.str),
    interpretationLastEvaluated = rawAssertion
      .extract("Interpretation", "@DateLastEvaluated")
      .flatMap(normalizeEvaluationDate),
    interpretationComments = rawAssertion
      .extract("Interpretation", "Comment")
      .fold(Array.empty[String])(normalizeComments),
    clinicalAssertionTraitSetId = clinicalAssertionTraitSetId,
    clinicalAssertionObservationIds = clinicalAssertionObservationIds,
    traitSetId = vcvTraitSetId,
    rcvAccessionId = rcvId
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
    * Process free-form comments to match our expected schema.
    *
    * SCV comments always contain a text body, and sometimes are tagged with a type.
    * A single SCV can contain zero+ comments. We normalize the comments to always
    * be an array of stringified JSON objects, each possessing a "text" field containing
    * the raw comment and an optional "type" field if the source comment had a
    * corresponding field.
    */
  def normalizeComments(rawComments: Msg): Array[String] = {
    def normalizeComment(comment: Msg) = {
      val base = Obj(Str("text") -> comment.value)
      upack
        .transform(
          comment.obj.get(Str("@Type")).fold(base) { commentType =>
            base.obj.update(Str("type"), commentType)
            base
          },
          StringRenderer()
        )
        .toString
    }

    rawComments match {
      case Arr(comments) => comments.map(normalizeComment).toArray
      case comment       => Array(normalizeComment(comment))
    }
  }

  /**
    * Helper method to extract the accession id from a raw SCV Msg.
    * Throws an exception if none is found, as that is an illegal state.
    */
  def extractAccessionId(rawScv: Msg): String = {
    rawScv
      .extract("ClinVarAccession", "@Accession")
      .getOrElse {
        throw new IllegalStateException(s"Found an SCV with no Accession ID: $rawScv")
      }
      .str
  }

  /**
    * Helper method to extract the numeric id from a raw SCV Msg.
    * Throws an exception if none is found, as that is an illegal state.
    */
  def extractNumericId(rawScv: Msg): String = {
    rawScv
      .extract("@ID")
      .getOrElse {
        throw new IllegalStateException(s"Found an SCV with no numeric ID: $rawScv")
      }
      .str
  }

  /**
    * Helper method to find the ID of the corresponding VCV Trait Set for the given SCV,
    * which is identified by the scvAccessionId.
    *
    * Given an SCV ID, we first find the corresponding SCVTraitSet. We then find the SCVTraits within
    * that SCVTraitSet, and find the clinvar traitIds that they point to. Using these clinvar traitIds,
    * we then find the VCVTraitSet that contains the VCVTraits that have the same clinvar traitIds. We
    * then return the ID of the found VCVTraitSet.
    */
  def findRelatedVcvTraitSetId(
    scvTraitSets: ArrayBuffer[WithContent[SCVTraitSet]],
    scvAccessionId: String,
    scvTraits: ArrayBuffer[WithContent[SCVTrait]],
    vcvTraitSets: ArrayBuffer[WithContent[VCVTraitSet]]
  ): Option[String] = {
    scvTraitSets
    // filter scvTraitSets down to the ones for the current scv
      .find(_.data.id == scvAccessionId)
      .flatMap { traitSet =>
        // need to use traitSet to find the right scv traits first
        val scvTraitIds = scvTraits.filter { `trait` =>
          traitSet.data.clinicalAssertionTraitIds.contains(`trait`.data.id)
        }.flatMap(_.data.traitId)
        // for that set of scv traits, compare the traitIds (the clinvar ones) to the vcvTraits.

        vcvTraitSets
        // filter vcvTraitSets down to the ones that have the same traits as the current scvTraitSet
          .find(_.data.traitIds.sameElements(scvTraitIds))
          .map { filteredSet =>
            // get the IDs of the relevant vcvTraitSets
            filteredSet.data.id
          }
      }
  }

  /**
    * Helper method to find the ID of the corresponding RCV.
    *
    * Given a VCVTraitSet ID, we find the RCV that it belongs to and return its ID.
    */
  def findRelatedRcvId(
    relevantVcvTraitSetId: Option[String],
    rcvs: ArrayBuffer[WithContent[RCV]]
  ): Option[String] = {

    rcvs.find { rcv =>
      // filter down to rcvs that contain the same traitSetIds (should be 1, no more no less)
      rcv.data.traitSetId.isDefined && rcv.data.traitSetId == relevantVcvTraitSetId
    }.map(_.data.id)
  }
}
