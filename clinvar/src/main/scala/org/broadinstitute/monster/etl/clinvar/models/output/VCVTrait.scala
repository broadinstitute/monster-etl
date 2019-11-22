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
  * @param medgenId unique ID of the trait in NCBI's MedGen database, if known
  * @param name full preferred name of the trait
  * @param alternateNames other names associated with the trait
  * @param `type` type of the trait
  * @param xrefs stringified JSON objects describing unique IDs for
  *              the trait in databases other than MedGen
  */
case class VCVTrait(
  id: String,
  medgenId: Option[String],
  name: Option[String],
  alternateNames: Array[String],
  `type`: Option[String],
  xrefs: Array[String]
)

object VCVTrait {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVTrait] = deriveEncoder(renaming.snakeCase, None)

  /** Extract a VCVTrait from a raw Trait payload. */
  def fromRawTrait(rawTrait: Msg): VCVTrait = {

    val allNames = MsgTransformations.popAsArray(rawTrait, "Name")

    val (preferredName, alternateNames, nameXrefs) =
      allNames.foldLeft((Option.empty[String], List.empty[String], List.empty[String])) {
        case ((prefAcc, altAcc, xrefAcc), name) =>
          val nameValue = name.extract("ElementValue").getOrElse {
            throw new IllegalStateException(s"Found a name with no value: $name")
          }
          val nameType = nameValue
            .extract("@Type")
            .getOrElse {
              throw new IllegalStateException(s"Found a name-value with no type: $name")
            }
            .str
          val nameString = nameValue.value

          val nameRefs = MsgTransformations
            .popAsArray(name, "XRef")
            .map { nameRef =>
              // Tag the ref with its source name so we can rebuild associations.
              nameRef.obj.update(Str("name"), nameString)
              processXref(nameRef)
            }
            .toList

          if (nameType == "Preferred") {
            if (prefAcc.isDefined) {
              throw new IllegalStateException(
                s"Trait $rawTrait has multiple preferred names"
              )
            } else {
              (Some(nameString.str), altAcc, nameRefs ::: xrefAcc)
            }
          } else {
            (prefAcc, nameString.str :: altAcc, nameRefs ::: xrefAcc)
          }
      }

    val topLevelRefs = MsgTransformations.popAsArray(rawTrait, "XRef")
    val (medgenId, finalXrefs) =
      topLevelRefs.foldLeft((Option.empty[String], nameXrefs)) {
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
            (medgenAcc, processXref(xref) :: xrefAcc)
          }
      }

    VCVTrait(
      id = rawTrait.extract("@ID").map(_.str).getOrElse {
        throw new IllegalStateException(s"Found a VCV Trait with no ID: $rawTrait")
      },
      medgenId = medgenId,
      name = preferredName,
      alternateNames = alternateNames.toArray,
      `type` = rawTrait.extract("@Type").map(_.str),
      xrefs = finalXrefs.toArray
    )
  }

  /**
    * Convert the keys of an XRef object to BQ-safe-snake-case, and stringify
    * it for storage in an array column.
    */
  def processXref(xref: Msg): String = {
    val cleaned = Obj()
    xref.obj.foreach {
      case (k, v) =>
        val cleanedKey =
          MsgTransformations.keyToSnakeCase(k.str.replaceAllLiterally("@", ""))
        cleaned.obj.update(Str(cleanedKey), v)
    }
    upack.transform(cleaned, StringRenderer()).toString
  }
}
