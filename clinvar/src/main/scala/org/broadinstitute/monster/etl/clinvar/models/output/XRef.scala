package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder
import upack.{Msg, Str}

case class XRef(db: String, id: String, sourceName: Option[String])

object XRef {

  // Jade doesn't support struct columns yet, so we output these as stringified JSONs.
  implicit val encoder: Encoder[XRef] =
    deriveEncoder.mapJson(obj => Json.fromString(obj.noSpaces))

  def fromRawXRef(rawXref: Msg, nameValue: Option[String] = Option.empty): XRef = {
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
        .str,
      // do we want to bother checking if it is an alternate name?
      nameValue
    )
  }
}
