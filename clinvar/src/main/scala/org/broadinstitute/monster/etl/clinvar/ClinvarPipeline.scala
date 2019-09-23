package org.broadinstitute.monster.etl.clinvar

import java.util.concurrent.atomic.AtomicInteger

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import cats.data.NonEmptyList
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import org.broadinstitute.monster.ClinvarBuildInfo
import org.broadinstitute.monster.etl.{MsgIO, UpackMsgCoder}
import upack._

import scala.collection.mutable

object ClinvarPipeline {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("ClinVar transformation pipeline")
  @AppVersion(ClinvarBuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
  case class Args(
    @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
    inputPrefix: String,
    @HelpMessage("Path where transformed ClinVar JSON should be written")
    outputPrefix: String
  )

  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${parsedArgs.inputPrefix}/VariationArchive/*.json"
      )

    val (rawVcvs, rawRcvs, rawVariations, rawScvs, rawScvVariations) =
      splitArchives(fullArchives)

    val vcvs = rawVcvs.map(
      mapFields(
        Map(
          NonEmptyList.of("@Accession") -> "accession",
          NonEmptyList.of("@Version") -> "version",
          NonEmptyList.of("@DateCreated") -> "date_created",
          NonEmptyList.of("@DateLastUpdated") -> "date_last_updated",
          NonEmptyList.of("@NumberOfSubmissions") -> "num_submissions",
          NonEmptyList.of("@NumberOfSubmitters") -> "num_submitters",
          NonEmptyList.of("RecordStatus") -> "record_status",
          NonEmptyList.of("InterpretedRecord", "ReviewStatus") -> "review_status",
          NonEmptyList.of("Species") -> "species",
          NonEmptyList.of("@ReleaseDate") -> "variation_archive_release_date"
        )
      )
    )
    val rcvs = rawRcvs.map(
      mapFields(
        Map(
          NonEmptyList.of("@Accession") -> "accession",
          NonEmptyList.of("@Version") -> "version",
          NonEmptyList.of("@Title") -> "title",
          NonEmptyList.of("@DateLastEvaluated") -> "date_last_evaluated",
          NonEmptyList.of("@ReviewStatus") -> "review_status",
          NonEmptyList.of("@Interpretation") -> "interpretation",
          NonEmptyList.of("@SubmissionCount") -> "submission_count",
          NonEmptyList.of("@independentObservations") -> "independent_observations"
        )
      )
    )
    val variations = rawVariations.map(
      mapFields(
        Map(
          NonEmptyList.of("@VariationID") -> "id",
          NonEmptyList.of("Name") -> "name",
          NonEmptyList.of("VariantType") -> "variation_type",
          NonEmptyList.of("@AlleleID") -> "allele_id",
          NonEmptyList.of("ProteinChange") -> "protein_change",
          NonEmptyList.of("@NumberOfChromosomes") -> "num_chromosomes",
          NonEmptyList.of("@NumberOfCopies") -> "num_copies"
        )
      )
    )
    val scvs = rawScvs.map(
      mapFields(
        Map(
          NonEmptyList.of("@DateCreated") -> "date_created",
          NonEmptyList.of("@DateLastUpdated") -> "date_last_updated",
          NonEmptyList.of("RecordStatus") -> "record_status",
          NonEmptyList.of("ReviewStatus") -> "review_status",
          NonEmptyList.of("@SubmissionDate") -> "submission_date",
          NonEmptyList.of("SubmissionNameList", "SubmissionName") -> "submission_names",
          NonEmptyList.of("ClinVarAccession", "@Accession") -> "accession",
          NonEmptyList.of("ClinVarAccession", "@Version") -> "version",
          NonEmptyList.of("ClinVarAccession", "@Type") -> "assertion_type",
          NonEmptyList.of("ClinVarAccession", "@OrgID") -> "org_id",
          NonEmptyList.of("ClinVarAccession", "@SubmitterName") -> "submitter_name",
          NonEmptyList.of("ClinVarAccession", "@OrganizationCategory") -> "org_category",
          NonEmptyList.of("ClinVarAccession", "@OrgAbbreviation") -> "org_abbrev",
          NonEmptyList.of("ClinVarSubmissionID", "@title") -> "title",
          NonEmptyList.of("ClinVarSubmissionID", "@localKey") -> "local_key",
          NonEmptyList
            .of("ClinVarSubmissionID", "@submittedAssembly") -> "submitted_assembly",
          NonEmptyList.of("Interpretation", "Description") -> "interp_description",
          NonEmptyList
            .of("Interpretation", "@DateLastEvaluated") -> "interp_date_last_evaluated",
          NonEmptyList.of("Interpretation", "Comment", "$") -> "interp_comment",
          NonEmptyList.of("Interpretation", "Comment", "@Type") -> "interp_comment_type"
        )
      )
    )
    val scvVariations = rawScvVariations.map(
      mapFields(Map(NonEmptyList.of("VariantType") -> "variation_type"))
    )

    val (clinicalAssertions, submitters, submissions) = splitScvs(scvs)

    MsgIO.writeJsonLists(
      vcvs,
      "VCV",
      s"${parsedArgs.outputPrefix}/variation_archive"
    )
    MsgIO.writeJsonLists(
      rcvs,
      "RCV Accession",
      s"${parsedArgs.outputPrefix}/rcv_accession"
    )
    MsgIO.writeJsonLists(
      variations,
      "Variation Archive Variation",
      s"${parsedArgs.outputPrefix}/variation_archive_variation"
    )
    MsgIO.writeJsonLists(
      clinicalAssertions,
      "SCV Clinical Assertion",
      s"${parsedArgs.outputPrefix}/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      submitters,
      "SCV Submitters",
      s"${parsedArgs.outputPrefix}/submitter"
    )
    MsgIO.writeJsonLists(
      submissions,
      "SCV Submissions",
      s"${parsedArgs.outputPrefix}/submission"
    )
    MsgIO.writeJsonLists(
      scvVariations,
      "SCV Clinical Assertion Variation",
      s"${parsedArgs.outputPrefix}/clinical_assertion_variation"
    )

    pipelineContext.close()
    ()
  }

  val idKey = Str("id")
  val vcvRef = Str("variation_archive_accession")
  val rcvRef = Str("rcv_accessions")
  val varRef = Str("variation_archive_variation_id")
  val parentVarRef = Str("parent_id")
  val parentVarsRef = Str("parent_ids")
  val scvRef = Str("clinical_assertion_accession")
  val submitterRef = Str("submitter_id")
  val submissionRef = Str("submission_id")
  val subclassKey = Str("subclass_type")

  val generatedKeys = Set[Msg](
    idKey,
    vcvRef,
    rcvRef,
    varRef,
    parentVarRef,
    parentVarsRef,
    scvRef,
    submitterRef,
    submissionRef,
    subclassKey
  )

  val interpretedRecord = Str("InterpretedRecord")
  val includedRecord = Str("IncludedRecord")

  def getVariant(message: Msg): Option[Msg] = {
    val subtypes = List("SimpleAllele", "Haplotype", "Genotype").map(Str)
    subtypes.foldLeft(Option.empty[Msg]) { (acc, subtype) =>
      acc.orElse {
        message.obj.remove(subtype).map {
          case arr @ Arr(msgs) =>
            msgs.foreach(_.obj.update(subclassKey, subtype))
            arr
          case msg =>
            msg.obj.update(subclassKey, subtype)
            msg
        }
      }
    }
  }

  def unrollVariants(
    variantWrapper: Msg,
    parentIds: List[Msg],
    getId: Msg => Msg,
    output: Msg => Unit
  ): Iterable[Msg] =
    getVariant(variantWrapper).toIterable.flatMap {
      case Arr(vs) => vs
      case other   => Iterable(other)
    }.map { variant =>
      val variantId = getId(variant)
      val immediateParentId = parentIds.headOption

      // Unroll any children.
      val _ = unrollVariants(variant, variantId :: parentIds, getId, output)

      // Link upwards.
      variant.obj.update(parentVarsRef, Arr(parentIds.to[mutable.ArrayBuffer]))
      immediateParentId.foreach(variant.obj.update(parentVarRef, _))

      // Output this fully-unrolled variant.
      output(variant)

      // Return this ID, in case the caller cares.
      variantId
    }

  def splitArchives(fullVcvStream: SCollection[Msg]): (
    SCollection[Msg],
    SCollection[Msg],
    SCollection[Msg],
    SCollection[Msg],
    SCollection[Msg]
  ) = {
    val rcvOut = SideOutput[Msg]
    val variationOut = SideOutput[Msg]
    val scvOut = SideOutput[Msg]
    val scvVariationOut = SideOutput[Msg]

    val (trimmedVcvStream, sideCtx) = fullVcvStream
      .withSideOutputs(
        rcvOut,
        variationOut,
        scvOut,
        scvVariationOut
      )
      .withName("Split Variation Archives")
      .map { (fullVcv, ctx) =>
        // Init top-level VCV info.
        val vcvObj = fullVcv.obj
        val vcvId = vcvObj(Str("@Accession"))
        val trimmedOut = new mutable.LinkedHashMap[Msg, Msg]

        // Pull out top-level record.
        // Fields which are destined for other tables will be stripped away.
        // Whatever's left behind will be re-added to the trimmed VCV to
        // preserve all information.
        def processRecord(originalRecord: Msg): Msg = {
          val recordCopy = upack.copy(originalRecord)

          // Extract and push out any RCVs in the VCV, tracking the collected IDs.
          val rcvIds = {
            val idBuffer = new mutable.ArrayBuffer[Msg]()
            val rcvs = for {
              wrapper <- recordCopy.obj.remove(Str("RCVList"))
              arrayOrSingle <- wrapper.obj.get(Str("RCVAccession"))
            } yield {
              arrayOrSingle match {
                case Arr(msgs) => msgs
                case msg       => Iterable(msg)
              }
            }
            rcvs.getOrElse(Iterable.empty).foreach { rcv =>
              val rcvObj = rcv.obj

              // Generate and track RCV ID.
              val id = rcvObj(Str("@Accession"))
              idBuffer.append(id)

              // Link back to VCV.
              rcvObj.update(vcvRef, vcvId)

              // Push to side output.
              ctx.output(rcvOut, rcv)
            }

            Arr(idBuffer)
          }

          // Extract and push top-level variant info from the VCV.
          val variationId = unrollVariants(
            recordCopy,
            Nil,
            _.obj(Str("@VariationID")),
            variant => {
              // Link back to the VCV and RCVs.
              variant.obj.update(vcvRef, vcvId)
              variant.obj.update(rcvRef, rcvIds)
              // Push to side output.
              ctx.output(variationOut, variant)
              ()
            }
          )

          // Extract and push out any SCVs and SCV-level variations.
          val scvs = for {
            wrapper <- recordCopy.obj.remove(Str("ClinicalAssertionList"))
            arrayOrSingle <- wrapper.obj.get(Str("ClinicalAssertion"))
          } yield {
            arrayOrSingle match {
              case Arr(msgs) => msgs
              case msg       => Iterable(msg)
            }
          }
          scvs.getOrElse(Iterable.empty).foreach { scv =>
            val scvObj = scv.obj
            val scvId = scvObj(Str("ClinVarAccession")).obj(Str("@Accession"))

            // Link the SCV back to the VCV, RCVs, and top-level variant.
            scvObj.update(vcvRef, vcvId)
            scvObj.update(rcvRef, rcvIds)
            variationId.foreach(scvObj.update(varRef, _))

            // Extract out any SCV-level variation.
            val counter = new AtomicInteger
            val _ = unrollVariants(
              scv,
              Nil,
              variant => {
                val nextId = Str(s"${scvId.str}.${counter.getAndIncrement()}")
                variant.obj.update(idKey, nextId)
                nextId
              },
              variant => {
                // Link the variation back to its SCV.
                variant.obj.update(scvRef, scvId)
                // Push to side output.
                ctx.output(scvVariationOut, variant)
                ()
              }
            )

            // Push SCV to side output.
            ctx.output(scvOut, scv)
          }

          recordCopy
        }

        // TODO useful comment
        vcvObj.foreach {
          case (k, v) =>
            if (k == interpretedRecord || k == includedRecord) {
              trimmedOut.update(k, processRecord(v))
            } else {
              trimmedOut.update(k, v)
            }
        }

        // Whatever's left in the VCV object is returned as the main output.
        Obj(trimmedOut): Msg
      }

    (
      trimmedVcvStream,
      sideCtx(rcvOut),
      sideCtx(variationOut),
      sideCtx(scvOut),
      sideCtx(scvVariationOut)
    )
  }

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

  def mapFields(mappings: Map[NonEmptyList[String], String])(msg: Msg): Msg = {
    val out = new mutable.LinkedHashMap[Msg, Msg]()
    val content = new mutable.LinkedHashMap[Msg, Msg]()

    msg.obj.foreach {
      case (k, v) =>
        // Track original fields in the 'content' block.
        if (generatedKeys.contains(k)) {
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

  def splitScvs(
    scvs: SCollection[Msg]
  ): (SCollection[Msg], SCollection[Msg], SCollection[Msg]) = {
    val submitterOut = SideOutput[Msg]
    val submissionOut = SideOutput[Msg]

    val (main, side) = scvs.withSideOutputs(submitterOut, submissionOut).map {
      (scv, ctx) =>
        val scvObj = scv.obj
        val newScv = new mutable.LinkedHashMap[Msg, Msg]()

        val scvId = scvObj(Str("accession"))
        val orgId = scvObj(Str("org_id"))
        val submitDate = scvObj(Str("submission_date"))
        val submissionId = Str(s"${orgId.str}.${submitDate.str}")

        val submitter = new mutable.LinkedHashMap[Msg, Msg]()
        val submission = new mutable.LinkedHashMap[Msg, Msg]()

        // Set and link IDs
        submitter.update(idKey, orgId)
        submission.update(idKey, submissionId)
        submission.update(submitterRef, orgId)
        newScv.update(idKey, scvId)
        newScv.update(submitterRef, orgId)
        newScv.update(submissionRef, submissionId)

        // Fill in other properties
        val submitterFields =
          Set("submitter_name", "org_category", "org_abbrev").map(Str)
        val submissionFields =
          Set("submission_date", "submission_names").map(Str)
        (scvObj.keySet -- submitterFields -- submissionFields - Str("org_id")).foreach {
          k =>
            scvObj.get(k).foreach(newScv.update(k, _))
        }
        submitterFields.foreach { k =>
          scvObj.get(k).foreach(submitter.update(k, _))
        }
        submissionFields.foreach { k =>
          scvObj.get(k).foreach(submission.update(k, _))
        }

        // Push outputs.
        ctx.output(submitterOut, Obj(submitter): Msg)
        ctx.output(submissionOut, Obj(submission): Msg)
        Obj(newScv): Msg
    }

    (
      main,
      side(submitterOut).distinctBy(_.obj(idKey).str),
      side(submissionOut).distinctBy(_.obj(idKey).str)
    )
  }
}
