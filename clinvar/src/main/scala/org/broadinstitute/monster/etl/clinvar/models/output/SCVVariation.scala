package org.broadinstitute.monster.etl.clinvar.models.output

import java.util.concurrent.atomic.AtomicInteger

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.clinvar.models.intermediate.WithContent
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import upack.{Arr, Msg}

import scala.collection.mutable

/**
  * Minimal info submitted to ClinVar about a variation.
  *
  * @param id unique ID for the submitted info
  * @param clinicalAssertionId accession of the SCV containing the info
  * @param subclassType subclass of variation described by the info (SimpleAllele, Haplotype, Genotype)
  * @param childIds unique IDs of the variations which naturally "nest" directly under this variation
  * @param descendantIds unique IDs of all variations which naturally "nest" under this
  *                      variation or any of its children
  * @param variationType type of this variation
  */
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

  /**
    * Extract all SCV variations from a raw ClinicalAssertion payload.
    *
    * ClinVar doesn't assign unique IDs to individual variation submissions, so there's
    * no meaningful way to deduplicate these variation records across SCVs. This means
    * that, unlike in the archive-level variation case, we extract and store the full
    * SCV variation tree for every SCV (instead of just the top-level variation).
    */
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

  /**
    * Traverse the tree of SCV variations, mapping and pushing each one.
    *
    * @param variationWrapper raw variation payload
    * @param output function which will store a mapped SCV variation model
    *               for later processing
    * @param map function which will convert a raw variation payload + subclass type
    *            into a mapped SCV variation model, paired with unmodeled content
    *
    * @return a tuple where the first element contains the IDs of the variation's
    *         immediate children, and the second contains the IDs of all other
    *         descendants of the variation
    */
  def extractTree(variationWrapper: Msg, output: WithContent[SCVVariation] => Unit)(
    map: (Msg, String) => SCVVariation
  ): (List[String], List[String]) = {
    val zero = (List.empty[String], List.empty[String])

    // Common logic for processing a single child of the variationWrapper.
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
