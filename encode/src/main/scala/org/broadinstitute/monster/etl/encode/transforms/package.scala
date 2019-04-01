package org.broadinstitute.monster.etl.encode

import com.spotify.scio.values.SCollection
import io.circe.JsonObject

package object transforms {

  /** Convenience alias for transformations that operate on JSON. */
  type JsonPipe = SCollection[JsonObject] => SCollection[JsonObject]
}
