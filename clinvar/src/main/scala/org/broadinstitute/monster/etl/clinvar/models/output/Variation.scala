package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.MsgTransformations
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import upack.{Arr, Msg}

/** TODO */
case class Variation(
  id: String,
  subclassType: String,
  childIds: Array[String],
  descendantIds: Array[String],
  name: Option[String],
  variationType: Option[String],
  alleleId: Option[String],
  proteinChange: Array[String],
  numberOfChromosomes: Option[Long],
  numberOfCopies: Option[Long]
)

object Variation {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[Variation] = deriveEncoder(renaming.snakeCase, None)

  /**
    * TODO
    *
    * NOTE: We expect that every variant destined for the Variant table will
    * be included in its own Variation Archive. To avoid processing duplicate
    * records, we only return the top-level variant returned from unrolling
    * the tree here.
    */
  def fromRawVariation(subtype: String, rawVariation: Msg): Variation = {
    // Collect relevant IDs from the variation and its descendants.
    val topId = extractId(rawVariation)
    val (childIds, descendantIds) = extractDescendantIds(rawVariation)

    // Map the remaining fields of the variation to our target columns.
    Variation(
      id = topId,
      subclassType = subtype,
      childIds = childIds.toArray,
      descendantIds = (childIds ::: descendantIds).toArray,
      name = rawVariation.extract("Name").map(_.str),
      variationType = rawVariation
        .extract("VariantType")
        .orElse(rawVariation.extract("VariationType"))
        .map(_.str),
      alleleId = rawVariation.extract("@AlleleID").map(_.str),
      proteinChange =
        MsgTransformations.popAsArray(rawVariation, "ProteinChange").map(_.str).toArray,
      numberOfChromosomes = rawVariation.extract("@NumberOfChromosomes").map(_.str.toLong),
      numberOfCopies = rawVariation.extract("@NumberOfCopies").map(_.str.toLong)
    )
  }

  /** TODO */
  def extractId(rawVariation: Msg): String =
    rawVariation
      .extract("@VariationID")
      .getOrElse {
        throw new IllegalStateException(s"Found a variation with no ID: $rawVariation")
      }
      .str

  /** TODO */
  def extractDescendantIds(rawVariation: Msg): (List[String], List[String]) = {
    val zero = (List.empty[String], List.empty[String])
    ClinvarConstants.VariationTypes.foldLeft(zero) {
      case ((childAcc, descendantsAcc), subtype) =>
        val (childIds, descendantIds) = rawVariation.obj.remove(subtype).fold(zero) {
          case Arr(children) =>
            children.foldLeft(zero) {
              case ((childAcc, descendantsAcc), child) =>
                val childId = extractId(child)
                val (grandchildIds, deepIds) = extractDescendantIds(child)
                (childId :: childAcc, grandchildIds ::: deepIds ::: descendantsAcc)
            }
          case child =>
            val childId = extractId(child)
            val (grandchildIds, deepIds) = extractDescendantIds(child)
            (List(childId), grandchildIds ::: deepIds)
        }
        (childIds ::: childAcc, descendantIds ::: descendantsAcc)
    }
  }
}
