package org.broadinstitute.monster.etl.clinvar.models.intermediate

import io.circe.{Encoder, Json}
import io.circe.syntax._
import ujson.StringRenderer
import upack.Msg

/**
  * Generic wrapper for a model which mixes strictly-parsed fields
  * with free-form unmodeled data.
  *
  * @param data the strictly-modeled data
  * @param content unmodeled data, stringified as a JSON object
  */
case class WithContent[D](data: D, content: Option[String])

object WithContent {

  implicit def encoder[D: Encoder]: Encoder[WithContent[D]] = dataAndContent => {
    val base = dataAndContent.data.asJson
    dataAndContent.content.fold(base) { content =>
      // Inject the content as a top-level field in the data as we write it out.
      base.deepMerge(Json.obj("content" -> content.asJson))
    }
  }

  /**
    * Bundle modeled and unmodeled data together.
    *
    * If the unmodeled data is empty, it will be dropped.
    */
  def attachContent[D](data: D, content: Msg): WithContent[D] = {
    val stringContent = if (content.obj.isEmpty) {
      None
    } else {
      Some(upack.transform(content, StringRenderer()).toString)
    }
    new WithContent[D](data, stringContent)
  }
}
