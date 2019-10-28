package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.Encoder
import io.circe.derivation.{deriveEncoder, renaming}
import upack.Msg

/** TODO */
case class VCVRelease(
  variationArchiveId: String,
  version: Long,
  releaseDate: String
)

object VCVRelease {
  import org.broadinstitute.monster.etl.clinvar.MsgOps

  implicit val encoder: Encoder[VCVRelease] = deriveEncoder(renaming.snakeCase, None)

  /** TODO */
  def fromRawArchive(vcv: VCV, rawArchive: Msg): VCVRelease =
    VCVRelease(
      variationArchiveId = vcv.id,
      version = vcv.version,
      releaseDate = rawArchive
        .extract("@ReleaseDate")
        .getOrElse {
          throw new IllegalStateException(
            s"Found a VCV with no release date: $rawArchive"
          )
        }
        .str
    )
}
