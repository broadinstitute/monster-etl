package org.broadinstitute.monster.etl.clinvar

import java.util.concurrent.atomic.AtomicInteger

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack._

import scala.collection.mutable

/** Container for functions used to split apart ClinVar's nested data. */
object ClinvarSplitters {

  import ClinvarContants._

  case class VariationTree(id: Msg, children: Seq[VariationTree])

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
    variantMessage: Msg
  )(getId: Msg => Msg): (Msg, List[Msg], List[Msg]) = {
    val immediateId = getId(variantMessage)
    val zero = (List.empty[Msg], List.empty[Msg])
    val (childIds, descendantIds) =
      getVariation(variantMessage).fold(zero) {
        case Arr(children) =>
          children.foldLeft(zero) {
            case ((childAcc, descandantsAcc), child) =>
              val (childId, grandChildIds, deepDescendants) =
                collectVariantIds(child)(getId)
              (childId :: childAcc, grandChildIds ::: deepDescendants ::: descandantsAcc)
          }
        case child =>
          val (childId, grandChildIds, deepDescendants) = collectVariantIds(child)(getId)
          (List(childId), grandChildIds ::: deepDescendants)
      }
    (immediateId, childIds, descendantIds)
  }

  /** TODO COMMENT */
  def splitArchives(fullVcvStream: SCollection[Msg])(implicit coder: Coder[Msg]): (
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
      .withName("Split VCVs")
      .flatMap { (fullVcv, ctx) =>
        val vcvObj = fullVcv.obj

        if (vcvObj.contains(InterpretedRecord) || vcvObj.contains(IncludedRecord)) {
          val recordCopy =
            upack.copy(vcvObj.getOrElse(InterpretedRecord, vcvObj(IncludedRecord)))
          val topLevelVariation = getVariation(recordCopy).getOrElse {
            throw new RuntimeException(s"Encountered VCV without a variant: $fullVcv")
          }
          val varObj = topLevelVariation.obj

          // Link the variant to its children.
          val (vcvVariationId, vcvVariationChildren, vcvVariationDescendants) =
            collectVariantIds(topLevelVariation)(_.obj(Str("@VariationID")))
          varObj.update(ChildrenRef, Arr(vcvVariationChildren: _*))
          varObj.update(
            DescendantsRef,
            Arr(vcvVariationChildren ::: vcvVariationDescendants: _*)
          )

          // Push out the variant.
          ctx.output(variationOut, topLevelVariation)

          if (vcvObj.contains(InterpretedRecord)) {
            val trimmedOut = new mutable.LinkedHashMap[Msg, Msg]
            val vcvId = vcvObj(Str("@Accession"))

            // Link the VCV to its variation.
            trimmedOut.update(VarRef, vcvVariationId)

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
              val (_, scvVariationChildren, scvVariationDescendants) =
                collectVariantIds(scvVariation) { scvVar =>
                  val id = Str(s"${scvId.str}.${counter.getAndIncrement()}")
                  scvVar.obj.update(IdKey, id)
                  id
                }
              // Link the variant to its children, subclass, and SCV.
              scvVariation.obj.update(ScvRef, scvId)
              scvVariation.obj.update(ChildrenRef, Arr(scvVariationChildren: _*))
              scvVariation.obj.update(
                DescendantsRef,
                Arr(scvVariationChildren ::: scvVariationDescendants: _*)
              )

              // Push SCV and variation to side outputs.
              ctx.output(scvVariationOut, scvVariation)
              ctx.output(scvOut, scv)
            }

            // Re-inject whatever fields are left in the record, along
            // with any top-level fields for the VCV.
            vcvObj.foreach {
              case (InterpretedRecord, _) =>
                trimmedOut.update(InterpretedRecord, recordCopy)
              case (k, v) => trimmedOut.update(k, v)
            }
            Iterable[Msg](Obj(trimmedOut))
          } else {
            // Included records are synthetic, so we don't bother recording them.
            Iterable.empty[Msg]
          }
        } else {
          throw new RuntimeException(s"Encountered VCV without a record: $fullVcv")
        }
      }

    (
      trimmedVcvStream,
      sideCtx(rcvOut),
      sideCtx(variationOut),
      sideCtx(scvOut),
      sideCtx(scvVariationOut)
    )
  }

  /** TODO COMMENT */
  def splitScvs(scvs: SCollection[Msg])(
    implicit coder: Coder[Msg]
  ): (SCollection[Msg], SCollection[Msg], SCollection[Msg]) = {
    val submitterOut = SideOutput[Msg]
    val submissionOut = SideOutput[Msg]

    val (main, side) =
      scvs.withSideOutputs(submitterOut, submissionOut).withName("Split SCVs").map {
        (scv, ctx) =>
          val scvObj = scv.obj
          val newScv = new mutable.LinkedHashMap[Msg, Msg]()

          val scvId = scvObj(IdKey)
          val orgId = scvObj(Str("org_id"))
          val submitDate = scvObj(Str("submission_date"))
          val submissionId = Str(s"${orgId.str}.${submitDate.str}")

          val submitter = new mutable.LinkedHashMap[Msg, Msg]()
          val submission = new mutable.LinkedHashMap[Msg, Msg]()

          // Set and link IDs
          submitter.update(IdKey, orgId)
          submission.update(IdKey, submissionId)
          submission.update(SubmitterRef, orgId)
          newScv.update(IdKey, scvId)
          newScv.update(SubmitterRef, orgId)
          newScv.update(SubmissionRef, submissionId)

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
      side(submitterOut).distinctBy(_.obj(IdKey).str),
      side(submissionOut).distinctBy(_.obj(IdKey).str)
    )
  }

  /** TODO COMMENT */
  def splitVcvs(vcvs: SCollection[Msg])(
    implicit coder: Coder[Msg]
  ): (SCollection[Msg], SCollection[Msg]) = {
    val releases = SideOutput[Msg]

    val (main, side) =
      vcvs.withSideOutputs(releases).withName("Split VCVs and releases").map {
        (vcv, ctx) =>
          val vcvCopy = upack.copy(vcv)
          val release = new mutable.LinkedHashMap[Msg, Msg]()

          val vcvId = vcvCopy.obj(IdKey)
          val vcvVersion = vcvCopy.obj(Str("version"))
          val vcvRelease = vcvCopy.obj(Str("release_date"))

          vcvCopy.obj.remove(Str("release_date"))

          release.update(VcvRef, vcvId)
          release.update(Str("version"), vcvVersion)
          release.update(Str("release_date"), vcvRelease)

          ctx.output(releases, Obj(release): Msg)
          vcvCopy
      }

    (main, side(releases))
  }
}
