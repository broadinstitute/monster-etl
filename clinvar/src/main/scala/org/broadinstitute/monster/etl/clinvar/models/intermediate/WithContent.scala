package org.broadinstitute.monster.etl.clinvar.models.intermediate

import io.circe.{Encoder, Json}
import io.circe.syntax._
import ujson.StringRenderer
import upack.Msg

/** TODO */
case class WithContent[D](data: D, content: Option[String])

object WithContent {

  implicit def encoder[D: Encoder]: Encoder[WithContent[D]] = dataAndContent => {
    val base = dataAndContent.data.asJson
    dataAndContent.content.fold(base) { content =>
      base.deepMerge(Json.obj("content" -> content.asJson))
    }
  }

  /** TODO */
  def attachContent[D](data: D, content: Msg): WithContent[D] = {
    val stringContent = if (content.obj.isEmpty) {
      None
    } else {
      Some(upack.transform(content, StringRenderer()).toString)
    }
    new WithContent[D](data, stringContent)
  }
}
