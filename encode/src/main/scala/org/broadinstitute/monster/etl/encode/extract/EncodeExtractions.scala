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
    entityType: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"extractEntities - $entryName") {
      _.flatMap { params =>
        //generic operation on SCollection[List[(String, String)]] for all steps of extractions
        client
          .search(
            entityType,
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
    _.transform(s"getExperimentSearchParams - $entryName") {
      for {
        assayType <- _
      } yield {
        List("assay_title" -> assayType)
      }
    }

  def extractExperimentSearchParams(
    entryName: String,
    entityType: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"extractExperimentSearchParams - $entryName") { collections =>
      extractEntities(entityType, entityType)(collections)
    }

  def getIDParams(
    entryName: String,
    referenceField: String,
    manyReferences: Boolean
  ): SCollection[JsonObject] => SCollection[String] =
    _.transform(s"getIDParams - $entryName") {
      _.map {
        _(referenceField)
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
    entityType: String
  ): SCollection[String] => SCollection[JsonObject] = { collections =>
    val extractIDs = collections.transform(s"extractIDParamEntities - $entryName") {
      collection =>
        collection.map { value =>
          KV.of("key", value)
        }.applyKvTransform(GroupIntoBatches.ofSize(100)).map { _.getValue.asScala }.map {
          _.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
            ("@id" -> ref) :: acc
          }
        }
    }
    extractEntities(entityType, entityType)(extractIDs)
  }
}
