package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
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

    val (vcvs, rcvs, variations, scvs, scvVariations) = splitTables(fullArchives)

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
      scvs,
      "SCV Combined (Clinical Assertion + Submission + Submitter)",
      s"${parsedArgs.outputPrefix}/scv_tmp"
    )
    MsgIO.writeJsonLists(
      scvVariations,
      "SCV Clinical Assertion Variation",
      s"${parsedArgs.outputPrefix}/clinical_assertion_variation"
    )
    MsgIO.writeJsonLists(
      variations,
      "Variation Archive Variation",
      s"${parsedArgs.outputPrefix}/variation_archive_variation"
    )

    pipelineContext.close()
    ()
  }

  val idKey = Str("id")
  val vcvRef = Str("variation_archive_id")
  val rcvRef = Str("rcv_accession_ids")
  val varRef = Str("variation_archive_variation_id")
  val parentVarRef = Str("parent_id")
  val parentVarsRef = Str("parent_ids")
  val scvRef = Str("clinical_assertion_id")

  val interpretedRecord = Str("InterpretedRecord")
  val includedRecord = Str("IncludedRecord")

  def getVariant(message: Msg): Option[Msg] = {
    message.obj
      .remove(Str("SimpleAllele"))
      .orElse(message.obj.remove(Str("Haplotype")))
      .orElse(message.obj.remove(Str("Genotype")))
  }

  def splitTables(fullVcvStream: SCollection[Msg]): (
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
        val vcvId =
          Str(s"${vcvObj(Str("@Accession")).str}.${vcvObj(Str("@Version")).str}")
        val trimmedOut = new mutable.LinkedHashMap[Msg, Msg]
        trimmedOut.update(idKey, vcvId)

        // Pull out top-level record.
        // Fields which are destined for other tables will be stripped away.
        // Whatever's left behind will be re-added to the trimmed VCV to
        // preserve all information.
        val record =
          vcvObj.get(interpretedRecord).orElse(vcvObj.get(includedRecord))
        val isInterpreted = vcvObj.contains(interpretedRecord)

        record.foreach { originalRecord =>
          val recordCopy = upack.copy(originalRecord)

          // Extract and push out any RCVs in the VCV, tracking the collected IDs.
          val rcvIds = new mutable.ArrayBuffer[Msg]()
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
            val id =
              Str(s"${rcvObj(Str("@Accession")).str}.${rcvObj(Str("@Version")).str}")
            rcvObj.update(idKey, id)
            rcvIds.append(id)

            // Link back to VCV.
            rcvObj.update(vcvRef, vcvId)

            // Push to side output.
            ctx.output(rcvOut, rcv)
          }

          // Extract and push archive-level variation records.
          // Variations are still nested when pushed out here.
          def unrollVariants(variantWrapper: Msg, parentIds: List[Msg]): Iterable[Msg] =
            getVariant(variantWrapper).toIterable.flatMap {
              case Arr(vs) => vs
              case other   => Iterable(other)
            }.map { variant =>
              val variantId = variant.obj(Str("@VariationID"))
              val immediateParentId = parentIds.headOption

              // Link upwards.
              variant.obj.update(vcvRef, vcvId)
              variant.obj.update(rcvRef, Arr(rcvIds))
              variant.obj.update(parentVarRef, Arr(parentIds.to[mutable.ArrayBuffer]))
              immediateParentId.foreach(variant.obj.update(parentVarsRef, _))

              // Continue unrolling.
              val _ = unrollVariants(variant, variantId :: parentIds)

              // Output this fully-unrolled variant.
              ctx.output(variationOut, variant)

              variantId
            }
          val variationId = unrollVariants(recordCopy, Nil).headOption

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
            val scvId = scvObj(Str("@ID"))

            // Link the SCV back to the VCV, RCVs, and top-level variant.
            scvObj.update(vcvRef, vcvId)
            scvObj.update(rcvRef, Arr(rcvIds))
            variationId.foreach(scvObj.update(varRef, _))

            // Extract out any SCV-level variation.
            val variation = getVariant(scv)
            variation.foreach { variant =>
              // Link the variation back to its SCV.
              variant.obj.update(scvRef, scvId)
              // Push to side output.
              ctx.output(scvVariationOut, variant)
            }

            // Push SCV to side output.
            ctx.output(scvOut, scv)
          }

          val recordKey = if (isInterpreted) interpretedRecord else includedRecord
          trimmedOut.update(recordKey, recordCopy)
        }

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
}
