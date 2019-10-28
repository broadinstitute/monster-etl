package org.broadinstitute.monster.etl.clinvar.models.output

import java.util.concurrent.atomic.AtomicInteger

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.WithContent
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import upack.{Arr, Msg}

import scala.collection.mutable

/** TODO */
case class SCVVariation(
  id: String,
  clinicalAssertionId: String,
  subclassType: String,
  childIds: Array[String],
  descendantIds: Array[String],
  variationType: Option[String]
)

object SCVVariation {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVVariation] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def allFromRawAssertion(
    scv: SCV,
    rawAssertion: Msg
  ): mutable.ArrayBuffer[WithContent[SCVVariation]] = {
    val buffer = new mutable.ArrayBuffer[WithContent[SCVVariation]]()
    // SCV variations don't have a pre-set ID, so we have to manufacture one.
    val counter = new AtomicInteger(0)

    // Process the tree of variations, if present.
    // Since SCV variations can't be meaningfully cross-linked between archives,
    // we have to extract the entire payload of every variant in the tree here,
    // instead of just their IDs.
    val _ = extractTree(rawAssertion, buffer.append(_)) { (rawVariation, subtype) =>
      SCVVariation(
        id = s"${scv.id}.${counter.getAndIncrement()}",
        clinicalAssertionId = scv.id,
        subclassType = subtype,
        childIds = Array.empty,
        descendantIds = Array.empty,
        variationType = rawVariation
          .extract("VariantType")
          .orElse(rawVariation.extract("VariationType"))
          .map(_.str)
      )
    }

    buffer
  }

  /** TODO */
  def extractTree(variationWrapper: Msg, output: WithContent[SCVVariation] => Unit)(
    map: (Msg, String) => SCVVariation
  ): (List[String], List[String]) = {
    val zero = (List.empty[String], List.empty[String])

    // TODO
    def processChild(rawChild: Msg, subtype: Msg): (String, List[String]) = {
      val baseVariation = map(rawChild, subtype.str)
      val (grandchildIds, deepIds) = extractTree(rawChild, output)(map)
      output(
        WithContent.attachContent(
          baseVariation.copy(
            childIds = grandchildIds.toArray,
            descendantIds = (grandchildIds ::: deepIds).toArray
          ),
          rawChild
        )
      )
      (baseVariation.id, grandchildIds ::: deepIds)
    }

    ClinvarConstants.VariationTypes.foldLeft(zero) {
      case ((childAcc, descendantsAcc), subtype) =>
        val (childIds, descendantIds) = variationWrapper.obj.remove(subtype).fold(zero) {
          case Arr(children) =>
            children.foldLeft(zero) {
              case ((childAcc, descendantsAcc), child) =>
                val (childId, descendantIds) = processChild(child, subtype)
                (childId :: childAcc, descendantIds ::: descendantsAcc)
            }
          case child =>
            val (childId, descendantIds) = processChild(child, subtype)
            (List(childId), descendantIds)
        }
        (childIds ::: childAcc, descendantIds ::: descendantsAcc)
    }
  }
}
