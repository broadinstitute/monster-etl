package org.broadinstitute.monster.etl.encode.extract

import io.circe.syntax._
import org.broadinstitute.monster.etl.encode._

object FileExtractions {

  /** Filter the files to make sure they are not any restricted or unavailable files. */
  val filterFiles: JsonPipe =
    _.transform("Filter Unavailable Files") {
      _.filter { jsonObj =>
        jsonObj("no_file_available").fold(true)(_.equals(false.asJson)) &&
        jsonObj("restricted").fold(true)(_.equals(false.asJson))
      }
    }
}
