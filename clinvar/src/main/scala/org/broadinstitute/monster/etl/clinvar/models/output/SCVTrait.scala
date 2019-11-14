package org.broadinstitute.monster.etl.clinvar.models.output

import java.util.concurrent.atomic.AtomicInteger

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.MsgTransformations
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import ujson.StringRenderer
import upack.{Msg, Obj, Str}

/**
  * Info about a trait included in a submission to ClinVar.
  *
  * @param id unique ID of the trait
  * @param clinicalAssertionTraitSetId unique ID of the collection which
  *                                    includes this trait
  * @param medgenId unique ID of the trait in NCBI's MedGen database, if known
  * @param name full name of the trait
  * @param `type` type of the trait
  * @param xrefs stringified JSON objects describing unique IDs for
  *              the trait in databases other than MedGen
  */
case class SCVTrait(
  id: String,
  clinicalAssertionTraitSetId: String,
  medgenId: Option[String],
  name: Option[String],
  `type`: Option[String],
  xrefs: Array[String],
  traitId: Option[String]
)

object SCVTrait {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTrait] = deriveEncoder(renaming.snakeCase, None)

  /** Extract an SCVTrait from a raw Trait payload. */
  def fromRawTrait(
    traitSet: SCVTraitSet,
    traits: Array[VCVTrait],
    traitMappings: Array[TraitMapping],
    counter: AtomicInteger,
    rawTrait: Msg
  ): SCVTrait = {
    val nameWrapper = rawTrait.extract("Name", "ElementValue")
    val nameType = rawTrait.extract("@Type").map(_.str)
    val allXrefs = MsgTransformations.popAsArray(rawTrait, "XRef")
    val (medgenId, xrefs) =
      allXrefs.foldLeft((Option.empty[String], List.empty[Msg])) {
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

            (medgenAcc, cleaned :: xrefAcc)
          }
      }

    val baseScv = SCVTrait(
      id = s"${traitSet.id}.${counter.getAndIncrement()}",
      clinicalAssertionTraitSetId = traitSet.id,
      medgenId = medgenId,
      name = nameWrapper.map(_.value.str),
      `type` = rawTrait.extract("@Type").map(_.str),
      xrefs = xrefs.toArray.map(upack.transform(_, StringRenderer()).toString),
      traitId = None
    )

    if (traitMappings.isEmpty) {
      baseScv.copy(traitId = traits.headOption.map(_.id))
    } else {
      // Find the MedGen ID to look for in the VCV traits.
      val matchingMedgenId = baseScv.medgenId.orElse {
        traitMappings.find { candidateMapping =>
          val sameTraitType =
            baseScv.`type`.contains(candidateMapping.traitType)

          val nameMatch = {
            val isNameMapping = candidateMapping.mappingType == "Name"
            val isPreferredMatch = candidateMapping.mappingRef == "Preferred" &&
              nameType.contains("Preferred") &&
              baseScv.name.contains(candidateMapping.mappingValue)
            val isAlternateMatch = candidateMapping.mappingRef == "Alternate" &&
              nameType.contains("included") &&
              baseScv.name.contains(candidateMapping.mappingValue)

            isNameMapping && (isPreferredMatch || isAlternateMatch)
          }

          val xrefMatch = {
            val isXrefMapping = candidateMapping.mappingType == "XRef"
            val xrefMatches = xrefs.exists { xref =>
              xref.obj(Str("db")).str == candidateMapping.mappingRef &&
              xref.obj(Str("id")).str == candidateMapping.mappingValue
            }

            isXrefMapping && xrefMatches
          }

          sameTraitType && (nameMatch || xrefMatch)
        }.flatMap(_.medgenId)
      }
      // Find the VCV trait with the MedGen ID.
      val matchingVcvTrait = traits.find(_.medgenId == matchingMedgenId)
      // Link!
      baseScv.copy(traitId = matchingVcvTrait.map(_.id))
    }
  }
}
