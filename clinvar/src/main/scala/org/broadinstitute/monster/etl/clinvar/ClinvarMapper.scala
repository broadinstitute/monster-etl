package org.broadinstitute.monster.etl.clinvar

import cats.data.NonEmptyList
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.etl.MsgTransformations
import ujson.StringRenderer
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable
import scala.util.matching.Regex

/** Container for functions used to map fields in ClinVar's data. */
class ClinvarMapper(implicit msgCoder: Coder[Msg]) extends Serializable {

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
        s"Attempted to extract field(s) [$fieldChain] from non-object: $nonObj"
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
        if (ClinvarConstants.GeneratedKeys.contains(k)) {
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
      val stringContent = upack.transform(Obj(content), StringRenderer())
      out.update(ClinvarConstants.UnmodeledContentKey, Str(stringContent.toString))
    }

    Obj(out): Msg
  }

  type Mapper = SCollection[Msg] => SCollection[Msg]

  val variationMappings = Map(
    NonEmptyList.of("@VariationID") -> ClinvarConstants.IdKey,
    NonEmptyList.of("Name") -> Str("name"),
    NonEmptyList.of("VariantType") -> Str("variation_type"),
    NonEmptyList.of("VariationType") -> Str("variation_type"),
    NonEmptyList.of("@AlleleID") -> Str("allele_id"),
    NonEmptyList.of("ProteinChange") -> Str("protein_change"),
    NonEmptyList.of("@NumberOfChromosomes") -> Str("num_chromosomes"),
    NonEmptyList.of("@NumberOfCopies") -> Str("num_copies")
  )

  /** Map the names and types of fields in raw variations into our desired schema. */
  val mapVariations: Mapper = _.transform("Cleanup Variations") {
    _.map { variation =>
      MsgTransformations.parseLongs(Set("allele_id", "num_chromosomes", "num_copies")) {
        MsgTransformations.ensureArrays(Set("protein_change")) {
          mapFields(variation, variationMappings)
        }
      }
    }
  }

  val geneMappings = Map(
    NonEmptyList.of("@GeneID") -> ClinvarConstants.IdKey,
    NonEmptyList.of("@Symbol") -> Str("symbol"),
    NonEmptyList.of("@HGNC_ID") -> Str("hgnc_id"),
    NonEmptyList.of("@FullName") -> Str("full_name"),
    NonEmptyList.of("@Source") -> Str("source"),
    NonEmptyList.of("@RelationshipType") -> Str("relationship_type")
  )

  /** Map the names and types of fields in raw genes into our desired schema. */
  val mapGenes: Mapper = _.transform("Cleanup Genes")(_.map(mapFields(_, geneMappings)))

  val vcvMappings = Map(
    NonEmptyList.of("@Accession") -> ClinvarConstants.IdKey,
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

  /** Map the names and types of fields in raw VCVs into our desired schema. */
  val mapVcvs: Mapper = _.transform("Cleanup VCVs") {
    _.map { vcv =>
      MsgTransformations.parseLongs(Set("version", "num_submissions", "num_submitters")) {
        mapFields(vcv, vcvMappings)
      }
    }
  }

  val rcvMappings = Map(
    NonEmptyList.of("@Accession") -> ClinvarConstants.IdKey,
    NonEmptyList.of("@Version") -> Str("version"),
    NonEmptyList.of("@Title") -> Str("title"),
    NonEmptyList.of("@DateLastEvaluated") -> Str("date_last_evaluated"),
    NonEmptyList.of("@ReviewStatus") -> Str("review_status"),
    NonEmptyList.of("@Interpretation") -> Str("interpretation"),
    NonEmptyList.of("@SubmissionCount") -> Str("submission_count"),
    NonEmptyList.of("@independentObservations") -> Str("independent_observations")
  )

  /** Map the names and types of fields in raw RCVs into our desired schema. */
  val mapRcvs: Mapper = _.transform("Cleanup RCVs") {
    _.map { rcv =>
      MsgTransformations.parseLongs(
        Set("version", "submission_count", "independent_observations")
      )(mapFields(rcv, rcvMappings))
    }
  }

  val scvMappings = Map(
    NonEmptyList.of("Assertion") -> Str("assertion_type"),
    NonEmptyList.of("@DateCreated") -> Str("date_created"),
    NonEmptyList.of("@DateLastUpdated") -> Str("date_last_updated"),
    NonEmptyList.of("RecordStatus") -> Str("record_status"),
    NonEmptyList.of("ReviewStatus") -> Str("review_status"),
    NonEmptyList.of("@SubmissionDate") -> Str("submission_date"),
    NonEmptyList.of("ClinVarAccession", "@Accession") -> ClinvarConstants.IdKey,
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
    NonEmptyList.of("Interpretation", "Comment") -> Str("interp_comments"),
    NonEmptyList
      .of("SubmissionNameList", "SubmissionName") -> Str("submission_names")
  )

  /**
    * Regex matching the YYYY-MM-DD portion of a date field which might also contain
    * a trailing timestamp.
    *
    * Used to normalize fields which are intended to be dates, not timestamps.
    */
  val DatePattern: Regex = """^(\d{4}-\d{2}-\d{2}).*""".r

  /** Map the names and types of fields in raw SCVs into our desired schema. */
  val mapScvs: Mapper = _.transform("Cleanup SCVs") { scvs =>
    def scvComment(commentType: Msg, commentText: Msg): Msg = {
      val commentObj = Obj(Str("type") -> commentType, Str("text") -> commentText)
      Str(upack.transform(commentObj, StringRenderer()).toString)
    }

    scvs.map { scv =>
      val mapped =
        MsgTransformations.ensureArrays(Set("submission_names", "interp_comments")) {
          mapFields(scv, scvMappings)
        }
      mapped.obj.remove(Str("interp_comments")).foreach { rawComments =>
        // SCV comments always contain a text body, and sometimes are tagged with a type.
        // If they have a type, they'll be extracted as objects.
        // Otherwise they'll be extracted as scalar strings.
        // We normalize them to always be stored as stringified JSON objects, with
        // an 'unknown' type when needed.
        val normalized = rawComments.arr.map {
          case Obj(fields) => scvComment(fields(Str("@Type")), fields(Str("$")))
          case other       => scvComment(Str("unknown"), other)
        }
        mapped.obj.update(Str("interp_comments"), Arr(normalized))
      }
      mapped.obj.remove(Str("interp_date_last_evaluated")).foreach { rawDate =>
        // interp_date_last_evaluated *sometimes* contains hour values, which breaks BQ.
        // The hour values aren't really important, so we strip them out when present.
        rawDate.str match {
          case DatePattern(trimmedDate) =>
            mapped.obj.update(Str("interp_date_last_evaluated"), Str(trimmedDate))
          case other =>
            logger.warn(s"Found un-parseable date [$other] in SCV: $mapped")
            ()
        }
      }
      mapped
    }
  }

  val scvVariationMappings = Map(
    NonEmptyList.of("VariantType") -> Str("variation_type"),
    NonEmptyList.of("VariationType") -> Str("variation_type")
  )

  /** Map the names and types of fields in raw SCV variations into our desired schema. */
  val mapScvVariations: Mapper =
    _.transform("Cleanup SCV Variations")(_.map(mapFields(_, scvVariationMappings)))

  // Purposefully empty; we want to dump everything in 'content' for now.
  val scvObservationMappings = Map.empty[NonEmptyList[String], Msg]

  /** Map the names and types of fields in raw SCV observations into our desired schema. */
  val mapScvObservations: Mapper =
    _.transform("Cleanup SCV Observations")(_.map(mapFields(_, scvObservationMappings)))

  val scvTraitSetMappings = Map(NonEmptyList.of("@Type") -> Str("type"))

  /** Map the names and types of fields in raw SCV trait sets into our desired schema. */
  val mapScvTraitSets: Mapper =
    _.transform("Cleanup SCV Trait Sets")(_.map(mapFields(_, scvTraitSetMappings)))

  val scvTraitMappings = Map(
    NonEmptyList.of("@Type") -> Str("type"),
    NonEmptyList.of("Name", "ElementValue", "$") -> Str("name")
  )

  /** Map the names and types of fields in raw SCV traits into our desired schema. */
  val mapScvTraits: Mapper =
    _.transform("Cleanup SCV Traits")(_.map(mapFields(_, scvTraitMappings)))
}
