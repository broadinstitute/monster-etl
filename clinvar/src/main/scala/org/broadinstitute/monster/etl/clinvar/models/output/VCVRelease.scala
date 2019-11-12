package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/**
  * Model capturing when a specific version of a variation archive
  * was released.
  *
  * @param variationArchiveId the ClinVar accession of the archive
  * @param version the specific version of the accession
  * @param releaseDate the day when the archive with `variationArchiveId`,
  *                    version `version`, was first released
  */
case class VCVRelease(
  variationArchiveId: String,
  version: Long,
  releaseDate: String
)

object VCVRelease {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVRelease] = deriveEncoder(renaming.snakeCase, None)

  /** Extract release-related info from a raw VariationArchive payload. */
  def fromRawArchive(vcv: VCV, rawArchive: Msg): VCVRelease =
    VCVRelease(
      variationArchiveId = vcv.id,
      version = vcv.version,
      releaseDate = rawArchive
        .extract("ClinVarVariationRelease", "@ReleaseDate")
        .getOrElse {
          throw new IllegalStateException(
            s"Found a VCV with no release date: $rawArchive"
          )
        }
        .str
    )
}
