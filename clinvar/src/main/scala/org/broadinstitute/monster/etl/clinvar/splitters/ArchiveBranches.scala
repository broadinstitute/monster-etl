package org.broadinstitute.monster.etl.clinvar.splitters

import java.util.concurrent.atomic.AtomicInteger

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable

/** TODO COMMENT */
case class ArchiveBranches(
  variations: SCollection[Msg],
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

    val vcvOut = SideOutput[Msg]
    val rcvOut = SideOutput[Msg]
    val scvOut = SideOutput[Msg]
    val scvVariationOut = SideOutput[Msg]

    val (variationStream, sideCtx) = archiveStream
      .withSideOutputs(
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
        val topLevelVariation = getVariation(recordCopy).getOrElse {
          throw new RuntimeException(s"Encountered VCV without a variant: $fullVcv")
        }

        // Link the variant to its children.
        // NOTE: The linking process pops child records out of their enclosing parents'
        // field sets. We assume that every curated variant will have a top-level
        // archive entry in the input, so we don't push the children as we pop them.
        val (vcvVariationId, _, _) =
          collectVariantIds(topLevelVariation, None)(_.obj(Str("@VariationID")))

        if (vcvObj.contains(InterpretedRecord)) {
          val trimmedVcv = new mutable.LinkedHashMap[Msg, Msg]
          val vcvId = vcvObj(Str("@Accession"))

          // Link the VCV to its variation.
          trimmedVcv.update(VarRef, vcvVariationId)

          // Extract and push out any RCVs in the VCV.
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

            // Link back to VCV and variation.
            rcvObj.update(VcvRef, vcvId)
            rcvObj.update(VarRef, vcvVariationId)

            // Push to side output.
            ctx.output(rcvOut, rcv)
          }

          // Extract any SCVs in the record.
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

            // Link the SCV back to the VCV, top-level variant, and RCVs.
            scvObj.update(VcvRef, vcvId)
            scvObj.update(VarRef, vcvVariationId)

            // Extract out SCV-level variation.
            val scvVariation = getVariation(scv).getOrElse {
              throw new RuntimeException(s"Encountered SCV without a variant: $scv")
            }
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
      vcvs = sideCtx(vcvOut),
      rcvs = sideCtx(rcvOut),
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut)
    )
  }

  /** TODO COMMENT */
  def getVariation(message: Msg): Option[Msg] =
    VariationTypes.foldLeft(Option.empty[Msg]) { (acc, subtype) =>
      acc.orElse(message.obj.remove(subtype).map {
        case arr @ Arr(many) =>
          many.foreach(_.obj.update(SubclassKey, subtype))
          arr
        case other =>
          other.obj.update(SubclassKey, subtype)
          other
      })
    }

  /** TODO COMMENT */
  def collectVariantIds(
    variantMessage: Msg,
    output: Option[Msg => Unit]
  )(getId: Msg => Msg): (Msg, List[Msg], List[Msg]) = {
    val immediateId = getId(variantMessage)
    val zero = (List.empty[Msg], List.empty[Msg])
    val (childIds, descendantIds) =
      getVariation(variantMessage).fold(zero) {
        case Arr(children) =>
          children.foldLeft(zero) {
            case ((childAcc, descandantsAcc), child) =>
              val (childId, grandChildIds, deepDescendants) =
                collectVariantIds(child, output)(getId)
              (childId :: childAcc, grandChildIds ::: deepDescendants ::: descandantsAcc)
          }
        case child =>
          val (childId, grandChildIds, deepDescendants) =
            collectVariantIds(child, output)(getId)
          (List(childId), grandChildIds ::: deepDescendants)
      }
    variantMessage.obj.update(ChildrenRef, Arr(childIds: _*))
    variantMessage.obj.update(DescendantsRef, Arr(childIds ::: descendantIds: _*))
    output.foreach(_.apply(variantMessage))
    (immediateId, childIds, descendantIds)
  }
}
