package org.broadinstitute.monster.etl

import com.spotify.scio.values.SCollection
import io.circe.JsonObject

package object encode {

  /** Convenience alias for transformations that operate on JSON. */
  type JsonPipe = SCollection[JsonObject] => SCollection[JsonObject]
}
