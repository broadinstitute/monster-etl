package org.broadinstitute.monster.etl.encode.extract

import io.circe.JsonObject
import org.broadinstitute.monster.etl.encode.extract.client.EncodeClient
import cats.implicits._
import org.apache.beam.sdk.transforms.GroupIntoBatches
import org.apache.beam.sdk.values.KV
import com.spotify.scio.values.SCollection
import scala.collection.JavaConverters._

/** Ingest step responsible for pulling raw metadata for a specific entity type from the ENCODE API. */
class EncodeExtractions(client: EncodeClient) {

  /**
    * add [("frame", "object"), ("status", "released")] to the parameters
    * and then call the ENCODE search client API using the query id/search parameters
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
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

  /**
    * gets the assay types for the experiment entity type and returns a list of tuples with "assay_title" -> assayType'
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    **/
  def getExperimentSearchParams(
    entryName: String
  ): SCollection[String] => SCollection[List[(String, String)]] =
    _.transform(s"Get $entryName experiment search parameters") {
      _.map { assayType =>
        List("assay_title" -> assayType)
      }
    }

  /**
    * given the experiemnt search parameters, query the ENCODE search client API
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
  def extractExperimentSearchParams(
    entryName: String,
    encodeApiName: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Extract $entryName experiment search parameters") { collections =>
      extractEntities(entryName, encodeApiName)(collections)
    }

  /**
    * given an entity type json's reference field , get each reference as a list of string id parameters
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param referenceField string containing reference values to query in the objects read in
    * @param manyReferences is the enitiy has more than one reference
    **/
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

  /**
    * batch references into groups of 100 using GroupIntoBatches,
    * then get list of tuples with "@id" -> reference
    * and then given those id parameters, query the ENCODE search client API
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
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
