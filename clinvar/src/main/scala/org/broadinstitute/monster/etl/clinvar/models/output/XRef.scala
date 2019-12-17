package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder
import upack.{Msg, Str}

case class XRef(db: String, id: String)

object XRef {

  // Jade doesn't support struct columns yet, so we output these as stringified JSONs.
  implicit val encoder: Encoder[XRef] =
    deriveEncoder.mapJson(obj => Json.fromString(obj.noSpaces))

  def fromRawXRef(rawXref: Msg): XRef = {
    XRef(
      rawXref.obj
        .getOrElse(
          Str("@DB"),
          throw new IllegalStateException(s"No DB tag found in xref: $rawXref")
        )
        .str,
      rawXref.obj
        .getOrElse(
          Str("@ID"),
          throw new IllegalStateException(s"No ID tag found in xref: $rawXref")
        )
        .str
    )
  }
}
