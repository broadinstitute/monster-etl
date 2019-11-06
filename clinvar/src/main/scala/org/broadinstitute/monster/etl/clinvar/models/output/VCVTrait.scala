package org.broadinstitute.monster.etl.clinvar.models.output

import java.util.concurrent.atomic.AtomicInteger

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.MsgTransformations
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import ujson.StringRenderer
import upack.{Msg, Obj, Str}

/**
  * Info about a trait approved by ClinVar.
  *
  * @param id unique ID of the trait, corresponding to ClinVar's internal TraitID
  * @param medgenTraitId unique ID of the trait in NCBI's MedGen database,
  *                      if known
  * @param name full name of the trait
  * @param `type` type of the trait
  */
case class VCVTrait(
                     id: String,
                     medgenTraitId: Option[String],
                     name: Option[String],
                     `type`: Option[String]
                   )

object VCVTrait {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVTrait] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a VCVTrait from a raw Trait payload. */
  def fromRawTrait(
                    traitSet: SCVTraitSet,
                    counter: AtomicInteger,
                    rawTrait: Msg
                  ): SCVTrait = {
    val nameWrapper = rawTrait.extract("Name", "ElementValue")
    val allXrefs = MsgTransformations.popAsArray(rawTrait, "XRef")
    val (medgenId, xrefs) =
      allXrefs.foldLeft((Option.empty[String], List.empty[String])) {
        case ((medgenAcc, xrefAcc), xref) =>
          val db = xref.obj.get(Str("@DB")).map(_.str)
          val id = xref.obj.get(Str("@ID")).map(_.str)

          if (db.contains(ClinvarConstants.MedGenKey)) {
            if (medgenAcc.isDefined) {
              throw new IllegalStateException(
                s"SCV Trait in set ${traitSet.id} contains two MedGen references"
              )
            } else {
              (id, xrefAcc)
            }
          } else {
            val cleaned = Obj()
            xref.obj.foreach {
              case (k, v) =>
                val cleanedKey =
                  MsgTransformations.keyToSnakeCase(k.str.replaceAllLiterally("@", ""))
                cleaned.obj.update(Str(cleanedKey), v)
            }
            val stringifiedRef = upack.transform(cleaned, StringRenderer()).toString

            (medgenAcc, stringifiedRef :: xrefAcc)
          }
      }

    SCVTrait(
      id = s"${traitSet.id}.${counter.getAndIncrement()}",
      clinicalAssertionTraitSetId = traitSet.id,
      medgenTraitId = medgenId,
      name = nameWrapper.flatMap(_.extract("$")).map(_.str),
      `type` = rawTrait.extract("@Type").map(_.str),
      xrefs = xrefs.toArray
    )
  }
}
