package org.broadinstitute.monster.etl.encode.extract

import io.circe.JsonObject
import org.broadinstitute.monster.etl.encode.extract.client.EncodeClient
import cats.implicits._
import org.apache.beam.sdk.transforms.GroupIntoBatches
import org.apache.beam.sdk.values.KV
import com.spotify.scio.values.SCollection
import scala.collection.JavaConverters._

class EncodeExtractions(client: EncodeClient) {

  def extractEntities(
    entryName: String,
    encodeApiName: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Extract $entryName entities") {
      _.flatMap { params =>
        //generic operation on SCollection[List[(String, String)]] for all steps of extractions
        client
          .search(
            encodeApiName,
            List("frame" -> "object", "status" -> "released") ::: params
          )
          .compile
          .toList
          .unsafeRunSync()
      }
    }

  def getExperimentSearchParams(
    entryName: String
  ): SCollection[String] => SCollection[List[(String, String)]] =
    _.transform(s"Get $entryName experiment search parameters") {
      _.map { assayType =>
        List("assay_title" -> assayType)
      }
    }

  def extractExperimentSearchParams(
    entryName: String,
    encodeApiName: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Extract $entryName experiment search parameters") { collections =>
      extractEntities(entryName, encodeApiName)(collections)
    }

  def getIDParams(
    entryName: String,
    referenceField: String,
    manyReferences: Boolean
  ): SCollection[JsonObject] => SCollection[String] =
    _.transform(s"Get $entryName id parameters") {
      _.map { json =>
        json(referenceField)
      }.collect {
        case Some(thing) =>
          thing
      }.flatMap { referenceJson =>
        val references = for {
          refValues <- if (manyReferences) {
            referenceJson.as[List[String]]
          } else {
            referenceJson.as[String].map { reference =>
              List(reference)
            }
          }
        } yield {
          refValues
        }
        references.valueOr(throw _)
      }
    }

  def extractIDParamEntities(
    entryName: String,
    encodeApiName: String
  ): SCollection[String] => SCollection[JsonObject] = { collections =>
    val extractIDs = collections.transform(s"Extract $entryName id parameter entities") {
      collection =>
        collection.map { value =>
          KV.of("key", value)
        }.applyKvTransform(GroupIntoBatches.ofSize(100)).map { _.getValue.asScala }.map {
          _.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
            ("@id" -> ref) :: acc
          }
        }
    }
    extractEntities(entryName, encodeApiName)(extractIDs)
  }
}
