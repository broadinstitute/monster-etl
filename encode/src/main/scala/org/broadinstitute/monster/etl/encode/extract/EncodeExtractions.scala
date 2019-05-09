package org.broadinstitute.monster.etl.encode.extract

import java.io.IOException

import com.google.common.util.concurrent.{
  Futures,
  ListenableFuture,
  MoreExecutors,
  SettableFuture
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.AsyncLookupDoFn.Try
import com.spotify.scio.transforms._
import org.broadinstitute.monster.etl.encode.transforms._
import io.circe.JsonObject
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import com.spotify.scio.values.SCollection
import com.squareup.okhttp.{Callback, OkHttpClient, Request, Response}
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}

import scala.collection.JavaConverters._
import scala.util.Success

class EncodeSearch(encodeApiName: String)
    extends AsyncLookupDoFn[List[(String, String)], String, AsyncHttpClient](100) {

  override def asyncLookup(
    client: AsyncHttpClient,
    params: List[(String, String)]
  ): ListenableFuture[String] = {
    val paramsString = List(
      "frame=object",
      "status=released",
      s"type=$encodeApiName",
      "limit=all",
      "format=json"
    ) ::: params.map {
      case (key, value) =>
        s"$key=$value"
    }

    Futures.transform(
      client.get(
        "https://www.encodeproject.org/search/?" + paramsString.mkString(sep = "&")
      ),
      (input: Option[String]) => input.get,
      MoreExecutors.directExecutor
    )
  }

  override protected def newClient(): AsyncHttpClient = new AsyncHttpClient
}

class AsyncHttpClient {

  def get(url: String): ListenableFuture[Option[String]] = {
    val request = new Request.Builder()
      .url(url)
      .get
      .build

    val result: SettableFuture[Option[String]] = SettableFuture.create()
    EncodeExtractions.client
      .newCall(request)
      .enqueue(new Callback() {
        def onFailure(request: Request, exception: IOException): Unit = {
          result.setException(exception)
          ()
        }

        def onResponse(response: Response): Unit = {
          if (response.isSuccessful) {
            result.set(Option(response.body.string))
            ()
          } else {
            result.set(Option("Default Value"))
            ()
          }
        }
      })
    result
  }
}

/** Ingest step responsible for pulling raw metadata for a specific entity type from the ENCODE API. */
object EncodeExtractions {

  val client = new OkHttpClient()

  implicit def coderTry: Coder[Try[String]] = Coder.kryo[Try[String]]

  /**
    * Add [("frame", "object"), ("status", "released")] to the parameters
    * and then call the ENCODE search client API using the query id/search parameters.
    *
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
  def extractEntities(
    entryName: String,
    encodeApiName: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Extract $entryName entities") {
      _.applyKvTransform(ParDo.of(new EncodeSearch(encodeApiName))).flatMap { kv =>
        kv.getValue.asScala match {
          case Success(value) =>
            io.circe.parser
              .parse(value)
              .flatMap { json =>
                json.as[Vector[JsonObject]]
              }
              .right
              .get
          case _ => ???
        }
      }
    }

  /**
    * Gets the assay types for the experiment entity type
    * and returns a list of tuples with "assay_title" -> assayType'.
    *
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

  // change var name (remove exp)
  /**
    * Given the search parameters, query the ENCODE search client API.
    *
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
  def extractSearchParams(
    entryName: String,
    encodeApiName: String
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Extract $entryName experiment search parameters") { collections =>
      extractEntities(entryName, encodeApiName)(collections)
    }

  /**
    * Given an entity type json's reference field
    * and get each reference as a list of string id parameters
    *
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param referenceField string containing reference values to query in the objects read in
    * @param manyReferences is the enitiy has more than one reference
    **/
  def getIDParams(
    entryName: String,
    referenceField: String,
    manyReferences: Boolean
  ): SCollection[JsonObject] => SCollection[String] =
    _.transform(s"Get $entryName id parameters") { collection =>
      collection.flatMap { jsonObj =>
        jsonObj(referenceField).toIterable.flatMap { referenceJson =>
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
          references.toOption
        }.flatten
      }.distinct
    }

  /**
    * Batch references into groups of 100 using GroupIntoBatches,
    * then get list of tuples with "@id" -> reference
    * and then given those id parameters, query the ENCODE search client API.
    *
    * @param entryName the name of the entity type to be displayed as a step within the pipeline
    * @param encodeApiName the entity type that will be queried
    **/
  def extractIDParamEntities(
    entryName: String,
    encodeApiName: String
  ): SCollection[String] => SCollection[JsonObject] = { collections =>
    val extractIDs = collections.transform(s"Extract $entryName id parameter entities") {
      collection =>
        collection
          .map(KV.of("key", _))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .applyKvTransform(GroupIntoBatches.ofSize(100))
          .map(_.getValue.asScala)
          .map {
            _.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
              ("@id" -> ref) :: acc
            }
          }
    }
    extractEntities(entryName, encodeApiName)(extractIDs)
  }
}
