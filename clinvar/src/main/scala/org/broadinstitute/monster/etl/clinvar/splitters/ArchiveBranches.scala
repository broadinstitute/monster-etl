package org.broadinstitute.monster.etl.clinvar.splitters

import java.util.concurrent.atomic.AtomicInteger

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable

/** TODO COMMENT */
case class ArchiveBranches(
  variations: SCollection[Msg],
  genes: SCollection[Msg],
  vcvs: SCollection[Msg],
  rcvs: SCollection[Msg],
  scvs: SCollection[Msg],
  scvVariations: SCollection[Msg]
)

object ArchiveBranches {
  import org.broadinstitute.monster.etl.clinvar.ClinvarContants._

  /** TODO COMMENT */
  def fromArchiveStream(
    archiveStream: SCollection[Msg]
  )(implicit coder: Coder[Msg]): ArchiveBranches = {

    val geneOut = SideOutput[Msg]
    val vcvOut = SideOutput[Msg]
    val rcvOut = SideOutput[Msg]
    val scvOut = SideOutput[Msg]
    val scvVariationOut = SideOutput[Msg]

    val (variationStream, sideCtx) = archiveStream
      .withSideOutputs(
        geneOut,
        vcvOut,
        rcvOut,
        scvOut,
        scvVariationOut
      )
      .withName("Split VCVs")
      .map { (fullVcv, ctx) =>
        val vcvObj = fullVcv.obj
        val recordCopy =
          upack.copy(vcvObj.getOrElse(InterpretedRecord, vcvObj(IncludedRecord)))

        // Pull out the variation for the archive.
        val topLevelVariation = getTopLevelVariation(recordCopy)
        // Link the variant to its children.
        // NOTE: The linking process pops child records out of their enclosing parents'
        // field sets. We assume that every curated variant will have a top-level
        // archive entry in the input, so we don't push the children as we pop them.
        val (vcvVariationId, _, _) =
          collectVariantIds(topLevelVariation, None)(_.obj(Str("@VariationID")))

        // Extract any genes associated with the variant.
        extractList(topLevelVariation, "GeneList", "Gene").foreach { gene =>
          gene.obj.update(VarRef, vcvVariationId)
          ctx.output(geneOut, gene)
        }

        // We only care about entities associated with "Interpreted" records,
        // which are backed by actual submissions.
        // The "Included" alternative is generated internally by ClinVar for DB consistency.
        if (vcvObj.contains(InterpretedRecord)) {
          val trimmedVcv = new mutable.LinkedHashMap[Msg, Msg]
          val vcvId = vcvObj(Str("@Accession"))

          // Link the VCV to its variation.
          trimmedVcv.update(VarRef, vcvVariationId)

          // Extract and push out any RCVs in the VCV.
          extractList(recordCopy, "RCVList", "RCVAccession").foreach { rcv =>
            val rcvObj = rcv.obj

            // Link back to VCV and variation.
            rcvObj.update(VcvRef, vcvId)
            rcvObj.update(VarRef, vcvVariationId)

            // Push to side output.
            ctx.output(rcvOut, rcv)
          }

          // Extract any SCVs in the record.
          // Extract and push out any SCVs and SCV-level variations.
          extractList(recordCopy, "ClinicalAssertionList", "ClinicalAssertion").foreach {
            scv =>
              val scvObj = scv.obj
              val scvId = scvObj(Str("ClinVarAccession")).obj(Str("@Accession"))

              // Link the SCV back to the VCV, top-level variant, and RCVs.
              scvObj.update(VcvRef, vcvId)
              scvObj.update(VarRef, vcvVariationId)

              // Extract out SCV-level variation.
              val scvVariation = getTopLevelVariation(scv)

              // SCV variations don't have a pre-set ID, so we have to manufacture one.
              val counter = new AtomicInteger(0)
              // Link SCV variations, pushing *each* variant.
              // There's no meaningful way to dedup variants across SCVs, so we just
              // capture all of them and live with the verbosity.
              val _ =
                collectVariantIds(scvVariation, Some { msg =>
                  val _ = ctx.output(scvVariationOut, msg)
                }) { scvVar =>
                  val id = Str(s"${scvId.str}.${counter.getAndIncrement()}")
                  scvVar.obj.update(IdKey, id)
                  // Link the variant to its SCV while we're at it.
                  scvVariation.obj.update(ScvRef, scvId)
                  id
                }

              // Push out the SCV.
              ctx.output(scvOut, scv)
          }

          // Re-inject whatever fields are left in the record, along
          // with any top-level fields for the VCV.
          vcvObj.foreach {
            case (InterpretedRecord, _) =>
              trimmedVcv.update(InterpretedRecord, recordCopy)
            case (k, v) => trimmedVcv.update(k, v)
          }
          ctx.output(vcvOut, Obj(trimmedVcv): Msg)
        }

        // Always output the top-level variation.
        topLevelVariation
      }

    ArchiveBranches(
      variations = variationStream,
      genes = sideCtx(geneOut),
      vcvs = sideCtx(vcvOut),
      rcvs = sideCtx(rcvOut),
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut)
    )
  }

  /** TODO COMMENT */
  def extractList(msg: Msg, wrapperName: String, elementName: String): Iterable[Msg] = {
    val maybeList = for {
      wrapper <- msg.obj.remove(Str(wrapperName))
      arrayOrSingle <- wrapper.obj.get(Str(elementName))
    } yield {
      arrayOrSingle match {
        case Arr(msgs) => msgs
        case msg       => Iterable(msg)
      }
    }

    maybeList.getOrElse(Iterable.empty)
  }

  /** TODO COMMENT */
  def getTopLevelVariation(msg: Msg): Msg =
    VariationTypes
      .foldLeft(Option.empty[Msg]) { (acc, subtype) =>
        acc.orElse(getVariation(msg, subtype))
      }
      .getOrElse {
        throw new RuntimeException(s"No variant found in entity: $msg")
      }

  /** TODO COMMENT */
  def getVariation(msg: Msg, subclassType: Msg): Option[Msg] =
    msg.obj.remove(subclassType).map {
      case arr @ Arr(many) =>
        many.foreach(_.obj.update(SubclassKey, subclassType))
        arr
      case other =>
        other.obj.update(SubclassKey, subclassType)
        other
    }

  /** TODO COMMENT */
  def collectVariantIds(
    variantMessage: Msg,
    output: Option[Msg => Unit]
  )(getId: Msg => Msg): (Msg, List[Msg], List[Msg]) = {
    val immediateId = getId(variantMessage)
    val zero = (List.empty[Msg], List.empty[Msg])
    val (childIds, descendantIds) = VariationTypes.foldLeft(zero) {
      case ((childAcc, descendandsAcc), subtype) =>
        val (children, descendants) = getVariation(variantMessage, subtype).fold(zero) {
          case Arr(children) =>
            children.foldLeft(zero) {
              case ((childAcc, descandantsAcc), child) =>
                val (childId, grandChildIds, deepDescendants) =
                  collectVariantIds(child, output)(getId)
                (
                  childId :: childAcc,
                  grandChildIds ::: deepDescendants ::: descandantsAcc
                )
            }
          case child =>
            val (childId, grandChildIds, deepDescendants) =
              collectVariantIds(child, output)(getId)
            (List(childId), grandChildIds ::: deepDescendants)
        }
        (children ::: childAcc, descendants ::: descendandsAcc)
    }
    variantMessage.obj.update(ChildrenRef, Arr(childIds: _*))
    variantMessage.obj.update(DescendantsRef, Arr(childIds ::: descendantIds: _*))
    output.foreach(_.apply(variantMessage))
    (immediateId, childIds, descendantIds)
  }
}
