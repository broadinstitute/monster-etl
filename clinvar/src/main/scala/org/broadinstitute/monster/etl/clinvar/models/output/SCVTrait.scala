package org.broadinstitute.monster.etl.clinvar.models.output

import java.util.concurrent.atomic.AtomicInteger

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import org.broadinstitute.monster.etl.MsgTransformations
import org.broadinstitute.monster.etl.clinvar.ClinvarConstants
import upack.Msg

/**
  * Info about a trait included in a submission to ClinVar.
  *
  * @param id unique ID of the trait
  *                                    includes this trait
  * @param medgenId unique ID of the trait in NCBI's MedGen database, if known
  * @param name full name of the trait
  * @param `type` type of the trait
  * @param xrefs stringified JSON objects describing unique IDs for
  *              the trait in databases other than MedGen
  * @param traitId the ClinVar trait ID of the corresponding VCVTrait, if known
  */
case class SCVTrait(
  id: String,
  medgenId: Option[String],
  name: Option[String],
  `type`: Option[String],
  xrefs: Array[XRef],
  traitId: Option[String]
)

object SCVTrait {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[SCVTrait] = deriveEncoder(renaming.snakeCase, None)

  /** Extract an SCVTrait from a raw Trait payload. */
  def fromRawTrait(
    idBase: String,
    traits: Array[VCVTrait],
    traitMappings: Array[TraitMapping],
    counter: AtomicInteger,
    rawTrait: Msg
  ): SCVTrait = {
    val nameWrapper = rawTrait.extract("Name", "ElementValue")
    val nameType = nameWrapper.flatMap(_.extract("@Type")).map(_.str)

    val allXrefs = MsgTransformations.popAsArray(rawTrait, "XRef").map(rawXref => XRef.fromRawXRef(rawXref))
    val (medgenId, xrefs) =
      allXrefs.foldLeft((Option.empty[String], List.empty[XRef])) {
        case ((medgenAcc, xrefAcc), xref) =>
          if (xref.db.contains(ClinvarConstants.MedGenKey)) {
            if (medgenAcc.isDefined) {
              throw new IllegalStateException(
                s"SCV Trait in set ${idBase} contains two MedGen references"
              )
            } else {
              (Option(xref.id), xrefAcc)
            }
          } else {
            (medgenAcc, xref :: xrefAcc)
          }
      }

    // Init all the fields we can pull by looking at the SCV in isolation.
    val baseScv = SCVTrait(
      id = s"${idBase}.${counter.getAndIncrement()}",
      medgenId = medgenId,
      name = nameWrapper.map(_.value.str),
      `type` = rawTrait.extract("@Type").map(_.str),
      xrefs = xrefs.toArray,
      traitId = None
    )

    // Look through the VCVs to see if there are any with aligned medgen IDs
    val medgenDirectMatch = traits.find(_.medgenId == baseScv.medgenId)

    // Look through the VCVs to see if there are any with aligned XRefs
    val xrefDirectMatch = traits.find(!_.xrefs.intersect(baseScv.xrefs).isEmpty)

    if (traitMappings.isEmpty) {
      // Lack of trait mappings means the VCV contains at most one trait,
      // which all the attached SCV traits should link to.
      baseScv.copy(traitId = traits.headOption.map(_.id))
    } else {
      // Find the VCV trait with the matching MedGen ID if it's defined.
      // Otherwise match on preferred name.
      val matchingVcvTrait = medgenDirectMatch
        .orElse(xrefDirectMatch)
        .orElse(findTraitMappingMatch(traitMappings, baseScv, xrefs, nameType, traits))

      // Link! Fill in the MegGen ID of the mapping too, if the SCV trait
      // didn't already contain one.
      baseScv.copy(
        traitId = matchingVcvTrait.map(_.id),
        medgenId = baseScv.medgenId.orElse(matchingVcvTrait.flatMap(_.medgenId))
      )
    }
  }

  /** Perform the logic needed to use the trait mappings to link an SCVTrait to a VCVTrait. */
  def findTraitMappingMatch(
    traitMappings: Array[TraitMapping],
    baseScv: SCVTrait,
    xrefs: List[XRef],
    nameType: Option[String],
    traits: Array[VCVTrait]
  ): Option[VCVTrait] = {
    // Look through the trait mappings for one that aligns with
    // the SCV's data.
    val matchingMapping = traitMappings.find { candidateMapping =>
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
          xref.db == candidateMapping.mappingRef &&
          xref.id == candidateMapping.mappingValue
        }

        isXrefMapping && xrefMatches
      }

      sameTraitType && (nameMatch || xrefMatch)
    }

    // Find the MedGen ID / name to look for in the VCV traits.
    val matchingMedgenId = matchingMapping.flatMap(_.medgenId)
    val matchingName = matchingMapping.flatMap(_.medgenName)

    // return the trait mapping medgen ID match or the name match
    traits
      .find(_.medgenId == matchingMedgenId)
      .orElse(traits.find(_.name == matchingName))
  }
}
