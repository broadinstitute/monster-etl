package org.broadinstitute.monster.etl.encode.extract

import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import org.broadinstitute.monster.etl.encode._

object AuditExtractions {

  /** Retain the Encode ID field ("@id") and Encode Audit field, ("audit") for Audits. */
  val transformAudits: SCollection[JsonObject] => SCollection[JsonObject] =
    _.transform("Extract Audit Info") {
      _.map { jsonObj =>
        Set("@id", "audit").foldLeft(JsonObject.empty) { (acc, field) =>
          jsonObj(field).fold(acc)(acc.add(field, _))
        }
      }

    }
}
