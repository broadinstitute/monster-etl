package org.broadinstitute.monster.etl.clinvar.models.output

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
  * @param medgenTraitId unique ID of the trait in NCBI's MedGen database, if known
  * @param name full name of the trait
  * @param `type` type of the trait
  */
case class VCVTrait(
  id: String,
  medgenTraitId: Option[String],
  name: Option[String],
  `type`: Option[String],
  otherXrefs: Array[String]
)

object VCVTrait {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVTrait] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a VCVTrait from a raw Trait payload. */
  def fromRawTrait(rawTrait: Msg): VCVTrait = {

    val preferredNameArray = MsgTransformations
      .getAsArray(rawTrait, "Name")
      .filter(_.obj(Str("ElementValue")).obj(Str("@Type")).str == "Preferred")
      .toArray

    if (preferredNameArray.length > 1) {
      throw new IllegalStateException(
        s"VCV Trait Set contains two Preferred names $rawTrait"
      )
    }

    val allXrefs = MsgTransformations.popAsArray(rawTrait, "XRef")
    val (medgenId, otherXrefs) =
      allXrefs.foldLeft((Option.empty[String], List.empty[String])) {
        case ((medgenAcc, xrefAcc), xref) =>
          val db = xref.obj.get(Str("@DB")).map(_.str)
          val id = xref.obj.get(Str("@ID")).map(_.str)

          if (db.contains(ClinvarConstants.MedGenKey)) {
            if (medgenAcc.isDefined) {
              throw new IllegalStateException(
                s"VCV Trait Set contains two MedGen references: $rawTrait"
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

    VCVTrait(
      id = rawTrait.extract("@ID").map(_.str).getOrElse {
        throw new IllegalStateException(s"Found a VCV Trait with no ID: $rawTrait")
      },
      medgenTraitId = medgenId,
      name = preferredNameArray.headOption
        .flatMap(_.extract("ElementValue").map(_.value.str)),
      `type` = rawTrait.extract("@Type").map(_.str),
      otherXrefs = otherXrefs.toArray
    )
  }
}
