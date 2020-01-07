package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder
import upack.{Msg, Str}

/**
  * Info about an XRef (cross-reference) in a trait.
  *
  * @param db the database it is from
  * @param id the id of it in the aforementioned database database
  * @param sourceName a human sensible name for it, if one is provided
  */
case class XRef(db: String, id: String, sourceName: Option[String])

object XRef {

  // Jade doesn't support struct columns yet, so we output these as stringified JSONs.
  implicit val encoder: Encoder[XRef] =
    deriveEncoder.mapJson(obj => Json.fromString(obj.noSpaces))

  /**
    * Extract an XRef from a rawXref and combine it with a name (if one is provided).
    *
    * @param rawXref a Msg containing the db and id information of the XRef
    * @param referencedName the human sensible name for the XRef, if one is provided
    * @return
    */
  def fromRawXRef(rawXref: Msg, referencedName: Option[String] = Option.empty): XRef = {
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
      referencedName
    )
  }
}
