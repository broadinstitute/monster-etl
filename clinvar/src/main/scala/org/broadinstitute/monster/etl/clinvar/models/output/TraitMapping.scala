package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Info about how "raw" traits included in individual submissions
  * should map into ClinVar's harmonized trait database.
  *
  * @param clinicalAssertionId unique ID of the SCV containing this mapping
  * @param traitType type of the submitted trait linked to this mapping
  * @param mappingType category of linking described by this mapping
  * @param mappingRef type-specific key which should be used to find the SCV
  *                   trait linked to this mapping
  * @param mappingValue type-specific value which should be used to find the
  *                     SCV trait linked to this mapping
  * @param medgenId unique ID of the SCV trait in NCBI's MedGen database, if known
  * @param medgenName long name of the SCV trait in NCBI's MedGen database, if known
  */
case class TraitMapping(
  // Fields required for linking mappings to SCV traits.
  clinicalAssertionId: String, // Also a foreign key.
  traitType: String,
  mappingType: String,
  mappingRef: String,
  mappingValue: String,
  // MedGen info.
  medgenId: Option[String],
  medgenName: Option[String]
)

object TraitMapping {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[TraitMapping] = deriveEncoder(renaming.snakeCase, None)

  /**
    * Extract a TraitMapping from a raw TraitMapping payload.
    *
    * NOTE: The `clinicalAssertionId` of the returned object is the numeric ID,
    * not the accession, since that's all we have access to within the raw input payload.
    */
  def fromRawMapping(rawMapping: Msg): TraitMapping = {
    val scvId = rawMapping
    // we use for the PK on the SCV table.
      .extract("@ClinicalAssertionID")
      .getOrElse {
        throw new IllegalStateException(
          s"Found a TraitMapping with no SCV ID: $rawMapping"
        )
      }
      .str
    val traitType = rawMapping
      .extract("@TraitType")
      .getOrElse {
        throw new IllegalStateException(
          s"Found a TraitMapping with no trait type: $rawMapping"
        )
      }
      .str
    val mappingType = rawMapping
      .extract("@MappingType")
      .getOrElse {
        throw new IllegalStateException(
          s"Found a TraitMapping with no mapping type: $rawMapping"
        )
      }
      .str
    val mappingKey = rawMapping
      .extract("@MappingRef")
      .getOrElse {
        throw new IllegalStateException(
          s"Found a TraitMapping with no mapping key: $rawMapping"
        )
      }
      .str
    val mappingValue = rawMapping
      .extract("@MappingValue")
      .getOrElse {
        throw new IllegalStateException(
          s"Found a TraitMapping with no mapping value: $rawMapping"
        )
      }
      .str

    TraitMapping(
      traitType = traitType,
      mappingType = mappingType,
      mappingValue = mappingValue,
      mappingRef = mappingKey,
      medgenId = rawMapping.extract("MedGen", "@CUI").map(_.str).filter(_ != "None"),
      medgenName = rawMapping.extract("MedGen", "@Name").map(_.str),
      clinicalAssertionId = scvId
    )
  }
}
