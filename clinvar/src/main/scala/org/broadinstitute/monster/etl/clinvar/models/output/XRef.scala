package org.broadinstitute.monster.etl.clinvar.models.output

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder

case class XRef(db: String, id: String)

object XRef {

  // Jade doesn't support struct columns yet, so we output these as stringified JSONs.
  implicit val encoder: Encoder[XRef] =
    deriveEncoder.mapJson(obj => Json.fromString(obj.noSpaces))
}
