package org.broadinstitute.monster.etl.encode.extract

import io.circe.JsonObject
import org.broadinstitute.monster.etl._
import org.broadinstitute.monster.etl.encode._

object AuditExtractions {

  /** Retain the Encode ID field ("@id") and Encode Audit field, ("audit") for Audits. */
  val transformAudits: JsonPipe =
    _.transform("Extract Audit Info") {
      _.map { jsonObj =>
        Set("@id", "audit").foldLeft(JsonObject.empty) { (acc, field) =>
          jsonObj(field).fold(acc)(acc.add(field, _))
        }
      }

    }
}
