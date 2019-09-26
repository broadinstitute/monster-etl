package org.broadinstitute.monster.etl.clinvar

import cats.data.NonEmptyList
import org.broadinstitute.monster.etl.MsgTransformations
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable

/** Container for functions used to map fields in ClinVar's data. */
object ClinvarMappers {

  /**
    * 'Drill' into a message, following a chain of fields, until either
    * the end of the chain is reached or an element of the chain is missing.
    *
    * @return the element at the end of the chain, if reachable
    */
  def drillDown(msg: Msg, fieldChain: List[String]): Option[Msg] =
    fieldChain match {
      case Nil => Some(msg)
      case f :: fs =>
        msg match {
          case Arr(msgs) =>
            Some(Arr(msgs.flatMap(drillDown(_, fs))))
          case Obj(props) =>
            props.get(Str(f)).flatMap(drillDown(_, fs))
          case _ => None
        }
    }

  /** Map a set of (possibly nested) fields to new names within a message. */
  def mapFields(msg: Msg, mappings: Map[NonEmptyList[String], String]): Msg = {
    val out = new mutable.LinkedHashMap[Msg, Msg]()
    val content = new mutable.LinkedHashMap[Msg, Msg]()

    msg.obj.foreach {
      case (k, v) =>
        // Track original fields in the 'content' block.
        if (ClinvarContants.GeneratedKeys.contains(k)) {
          out.update(k, v)
        } else {
          content.update(k, v)
        }

        mappings.foreach {
          case (chain, newName) =>
            if (Str(chain.head) == k) {
              drillDown(v, chain.tail).foreach(out.update(Str(newName), _))
            }
        }
    }

    val stringContent = upack.transform(Obj(content), ujson.StringRenderer())
    out.update(Str("content"), Str(stringContent.toString))

    Obj(out): Msg
  }

  val idName: String = ClinvarContants.IdKey.str

  /** Map the names and types of fields in a raw VCV into our desired schema. */
  def mapVcv(vcv: Msg): Msg =
    MsgTransformations.parseLongs(Set("version", "num_submissions", "num_submitters")) {
      mapFields(
        vcv,
        Map(
          NonEmptyList.of("@Accession") -> idName,
          NonEmptyList.of("@Version") -> "version",
          NonEmptyList.of("@DateCreated") -> "date_created",
          NonEmptyList.of("@DateLastUpdated") -> "date_last_updated",
          NonEmptyList.of("@NumberOfSubmissions") -> "num_submissions",
          NonEmptyList.of("@NumberOfSubmitters") -> "num_submitters",
          NonEmptyList.of("RecordStatus") -> "record_status",
          NonEmptyList.of("InterpretedRecord", "ReviewStatus") -> "review_status",
          NonEmptyList.of("Species") -> "species",
          NonEmptyList.of("@ReleaseDate") -> "release_date"
        )
      )
    }

  /** Map the names and types of fields in a raw RCV into our desired schema. */
  def mapRcv(rcv: Msg): Msg =
    MsgTransformations.parseLongs(
      Set("version", "submission_count", "independent_observations")
    ) {
      mapFields(
        rcv,
        Map(
          NonEmptyList.of("@Accession") -> idName,
          NonEmptyList.of("@Version") -> "version",
          NonEmptyList.of("@Title") -> "title",
          NonEmptyList.of("@DateLastEvaluated") -> "date_last_evaluated",
          NonEmptyList.of("@ReviewStatus") -> "review_status",
          NonEmptyList.of("@Interpretation") -> "interpretation",
          NonEmptyList.of("@SubmissionCount") -> "submission_count",
          NonEmptyList.of("@independentObservations") -> "independent_observations"
        )
      )
    }

  /** Map the names and types of fields in a raw VCV variation into our desired schema. */
  def mapVcvVariation(vcvVar: Msg): Msg =
    MsgTransformations.parseLongs(Set("allele_id", "num_chromosomes", "num_copies")) {
      MsgTransformations.ensureArrays(Set("protein_change")) {
        mapFields(
          vcvVar,
          Map(
            NonEmptyList.of("@VariationID") -> idName,
            NonEmptyList.of("Name") -> "name",
            NonEmptyList.of("VariantType") -> "variation_type",
            NonEmptyList.of("VariationType") -> "variation_type",
            NonEmptyList.of("@AlleleID") -> "allele_id",
            NonEmptyList.of("ProteinChange") -> "protein_change",
            NonEmptyList.of("@NumberOfChromosomes") -> "num_chromosomes",
            NonEmptyList.of("@NumberOfCopies") -> "num_copies"
          )
        )
      }
    }

  /** Map the names and types of fields in a raw SCV into our desired schema. */
  def mapScv(scv: Msg): Msg =
    MsgTransformations.ensureArrays(Set("submission_names")) {
      mapFields(
        scv,
        Map(
          NonEmptyList.of("Assertion") -> "assertion_type",
          NonEmptyList.of("@DateCreated") -> "date_created",
          NonEmptyList.of("@DateLastUpdated") -> "date_last_updated",
          NonEmptyList.of("RecordStatus") -> "record_status",
          NonEmptyList.of("ReviewStatus") -> "review_status",
          NonEmptyList.of("@SubmissionDate") -> "submission_date",
          NonEmptyList.of("ClinVarAccession", "@Accession") -> idName,
          NonEmptyList.of("ClinVarAccession", "@Version") -> "version",
          NonEmptyList.of("ClinVarAccession", "@OrgID") -> "org_id",
          NonEmptyList.of("ClinVarAccession", "@SubmitterName") -> "submitter_name",
          NonEmptyList
            .of("ClinVarAccession", "@OrganizationCategory") -> "org_category",
          NonEmptyList.of("ClinVarAccession", "@OrgAbbreviation") -> "org_abbrev",
          NonEmptyList.of("ClinVarSubmissionID", "@title") -> "title",
          NonEmptyList.of("ClinVarSubmissionID", "@localKey") -> "local_key",
          NonEmptyList
            .of("ClinVarSubmissionID", "@submittedAssembly") -> "submitted_assembly",
          NonEmptyList.of("Interpretation", "Description") -> "interp_description",
          NonEmptyList
            .of("Interpretation", "@DateLastEvaluated") -> "interp_date_last_evaluated",
          NonEmptyList.of("Interpretation", "Comment", "$") -> "interp_comment",
          NonEmptyList
            .of("Interpretation", "Comment", "@Type") -> "interp_comment_type",
          NonEmptyList
            .of("SubmissionNameList", "SubmissionName") -> "submission_names"
        )
      )
    }

  /** Map the names and types of fields in a raw SCV variation into our desired schema. */
  def mapScvVariation(scvVar: Msg): Msg =
    mapFields(
      scvVar,
      Map(
        NonEmptyList.of("VariantType") -> "variation_type",
        NonEmptyList.of("VariationType") -> "variation_type"
      )
    )
}
