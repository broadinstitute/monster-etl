package org.broadinstitute.monster.etl.clinvar

import cats.data.NonEmptyList
import org.broadinstitute.monster.etl.MsgTransformations
import upack.{Msg, Obj, Str}

import scala.collection.mutable

/** Container for functions used to map fields in ClinVar's data. */
object ClinvarMappers {

  val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
    * 'Drill' into a message, following a chain of fields, until either
    * the end of the chain is reached or an element of the chain is missing.
    *
    * @return the element at the end of the chain, if reachable
    */
  def drillDown(msg: Msg, fieldChain: NonEmptyList[String]): Option[Msg] = msg match {
    case Obj(fields) =>
      val firstKey = Str(fieldChain.head)
      NonEmptyList.fromList(fieldChain.tail) match {
        case None =>
          fields.remove(firstKey)
        case Some(remainingFields) =>
          fields.get(firstKey).flatMap { nested =>
            val retVal = drillDown(nested, remainingFields)
            nested match {
              case Obj(nestedFields) if nestedFields.isEmpty =>
                fields.remove(firstKey)
                ()
              case _ => ()
            }
            retVal
          }
      }
    // NOTE: In the case when an XML tag contains both attributes and a text value,
    //  our extractor program assigns the text value to the '$' field. When attributes
    // are optional for a tag, this results in a mix of value types for that tag in
    // the output JSON (some objects w/ '$' fields, and some scalars).
    // We try to handle both cases by allowing drill-down paths that end in '$' to
    // terminate one hop early.
    case nonObj if fieldChain == NonEmptyList("$", Nil) =>
      logger.warn(s"Returning value $nonObj in place of '$$' field")
      Some(nonObj)
    case nonObj =>
      logger.warn(
        s"Attempted to extract field(s) [${fieldChain}] from non-object: $nonObj"
      )
      None
  }

  /**
    * Map a set of (possibly nested) fields to new names within a message.
    *
    * Instances of our generated fields will be passed through with their
    * original names. Otherwise any unmapped fields will be consolidated
    * into a single string field containing a serialized JSON object.
    */
  def mapFields(msg: Msg, mappings: Map[NonEmptyList[String], Msg]): Msg = {
    val inCopy = upack.copy(msg)
    val out = new mutable.LinkedHashMap[Msg, Msg]()
    val content = new mutable.LinkedHashMap[Msg, Msg]()

    // Move any mapped fields over.
    mappings.foreach {
      case (chain, newName) =>
        drillDown(inCopy, chain).foreach(out.update(newName, _))
    }

    inCopy.obj.foreach {
      case (k, v) =>
        if (ClinvarContants.GeneratedKeys.contains(k)) {
          // Pass through fields generated during splitting.
          out.update(k, v)
        } else {
          // Don't throw away any info. If the field isn't mapped, add
          // it to the unmodeled content.
          content.update(k, v)
        }
    }

    // Stringify and store unmodeled fields, if there are any.
    if (content.nonEmpty) {
      val stringContent = upack.transform(Obj(content), ujson.StringRenderer())
      out.update(ClinvarContants.UnmodeledContentKey, Str(stringContent.toString))
    }

    Obj(out): Msg
  }

  val variationMappings = Map(
    NonEmptyList.of("@VariationID") -> ClinvarContants.IdKey,
    NonEmptyList.of("Name") -> Str("name"),
    NonEmptyList.of("VariantType") -> Str("variation_type"),
    NonEmptyList.of("VariationType") -> Str("variation_type"),
    NonEmptyList.of("@AlleleID") -> Str("allele_id"),
    NonEmptyList.of("ProteinChange") -> Str("protein_change"),
    NonEmptyList.of("@NumberOfChromosomes") -> Str("num_chromosomes"),
    NonEmptyList.of("@NumberOfCopies") -> Str("num_copies")
  )

  /** Map the names and types of fields in a raw variation into our desired schema. */
  def mapVariation(variation: Msg): Msg =
    MsgTransformations.parseLongs(Set("allele_id", "num_chromosomes", "num_copies")) {
      MsgTransformations.ensureArrays(Set("protein_change")) {
        mapFields(variation, variationMappings)
      }
    }

  val vcvMappings = Map(
    NonEmptyList.of("@Accession") -> ClinvarContants.IdKey,
    NonEmptyList.of("@Version") -> Str("version"),
    NonEmptyList.of("@DateCreated") -> Str("date_created"),
    NonEmptyList.of("@DateLastUpdated") -> Str("date_last_updated"),
    NonEmptyList.of("@NumberOfSubmissions") -> Str("num_submissions"),
    NonEmptyList.of("@NumberOfSubmitters") -> Str("num_submitters"),
    NonEmptyList.of("RecordStatus") -> Str("record_status"),
    NonEmptyList.of("InterpretedRecord", "ReviewStatus") -> Str("review_status"),
    NonEmptyList.of("Species") -> Str("species"),
    NonEmptyList.of("@ReleaseDate") -> Str("release_date")
  )

  /** Map the names and types of fields in a raw VCV into our desired schema. */
  def mapVcv(vcv: Msg): Msg =
    MsgTransformations.parseLongs(Set("version", "num_submissions", "num_submitters")) {
      mapFields(vcv, vcvMappings)
    }

  val rcvMappings = Map(
    NonEmptyList.of("@Accession") -> ClinvarContants.IdKey,
    NonEmptyList.of("@Version") -> Str("version"),
    NonEmptyList.of("@Title") -> Str("title"),
    NonEmptyList.of("@DateLastEvaluated") -> Str("date_last_evaluated"),
    NonEmptyList.of("@ReviewStatus") -> Str("review_status"),
    NonEmptyList.of("@Interpretation") -> Str("interpretation"),
    NonEmptyList.of("@SubmissionCount") -> Str("submission_count"),
    NonEmptyList.of("@independentObservations") -> Str("independent_observations")
  )

  /** Map the names and types of fields in a raw RCV into our desired schema. */
  def mapRcv(rcv: Msg): Msg =
    MsgTransformations.parseLongs(
      Set("version", "submission_count", "independent_observations")
    )(mapFields(rcv, rcvMappings))

  val scvMappings = Map(
    NonEmptyList.of("Assertion") -> Str("assertion_type"),
    NonEmptyList.of("@DateCreated") -> Str("date_created"),
    NonEmptyList.of("@DateLastUpdated") -> Str("date_last_updated"),
    NonEmptyList.of("RecordStatus") -> Str("record_status"),
    NonEmptyList.of("ReviewStatus") -> Str("review_status"),
    NonEmptyList.of("@SubmissionDate") -> Str("submission_date"),
    NonEmptyList.of("ClinVarAccession", "@Accession") -> ClinvarContants.IdKey,
    NonEmptyList.of("ClinVarAccession", "@Version") -> Str("version"),
    NonEmptyList.of("ClinVarAccession", "@OrgID") -> Str("org_id"),
    NonEmptyList.of("ClinVarAccession", "@SubmitterName") -> Str("submitter_name"),
    NonEmptyList
      .of("ClinVarAccession", "@OrganizationCategory") -> Str("org_category"),
    NonEmptyList.of("ClinVarAccession", "@OrgAbbreviation") -> Str("org_abbrev"),
    NonEmptyList.of("ClinVarSubmissionID", "@title") -> Str("title"),
    NonEmptyList.of("ClinVarSubmissionID", "@localKey") -> Str("local_key"),
    NonEmptyList
      .of("ClinVarSubmissionID", "@submittedAssembly") -> Str("submitted_assembly"),
    NonEmptyList.of("Interpretation", "Description") -> Str("interp_description"),
    NonEmptyList
      .of("Interpretation", "@DateLastEvaluated") -> Str("interp_date_last_evaluated"),
    NonEmptyList.of("Interpretation", "Comment", "$") -> Str("interp_comment"),
    NonEmptyList
      .of("Interpretation", "Comment", "@Type") -> Str("interp_comment_type"),
    NonEmptyList
      .of("SubmissionNameList", "SubmissionName") -> Str("submission_names")
  )

  /** Map the names and types of fields in a raw SCV into our desired schema. */
  def mapScv(scv: Msg): Msg =
    MsgTransformations.ensureArrays(Set("submission_names")) {
      mapFields(scv, scvMappings)
    }

  val scvVariationMappings = Map(
    NonEmptyList.of("VariantType") -> Str("variation_type"),
    NonEmptyList.of("VariationType") -> Str("variation_type"),
    NonEmptyList.of("GeneList", "Gene", "@Symbol") -> ClinvarContants.GeneRef
  )

  /** Map the names and types of fields in a raw SCV variation into our desired schema. */
  def mapScvVariation(scvVar: Msg): Msg =
    mapFields(scvVar, scvVariationMappings)
}
