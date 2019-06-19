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
import com.spotify.scio.values.SCollection
import com.squareup.okhttp.{Callback, OkHttpClient, Request, Response}
import io.circe.JsonObject
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.etl._
import org.broadinstitute.monster.etl.encode._

import scala.collection.JavaConverters._

/** Ingest step responsible for pulling raw metadata for a specific entity type from the ENCODE API. */
object EncodeExtractions {

  /** Boilerplate needed to tell scio how to (de)serialize its internal Try type. */
  implicit def coderTry: Coder[Try[String]] = Coder.kryo[Try[String]]

  /** HTTP client to use for querying ENCODE APIs. */
  val client = new OkHttpClient()

  /**
    * Pipeline stage which maps batches of query params to output payloads
    * by sending the query params to the ENCODE search API.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  class EncodeLookup(encodeEntity: EncodeEntity)
      extends AsyncLookupDoFn[List[(String, String)], String, OkHttpClient] {

    private val baseParams =
      List("frame=object", "status=released", "limit=all", "format=json")

    override def asyncLookup(
      client: OkHttpClient,
      params: List[(String, String)]
    ): ListenableFuture[String] = {
      val paramStrings = params.map {
        case (key, value) =>
          s"$key=$value"
      }
      val allParams = s"type=${encodeEntity.encodeApiName}" :: baseParams ::: paramStrings

      Futures.transform(
        get(
          client,
          s"https://www.encodeproject.org/search/?${allParams.mkString(sep = "&")}"
        ),
        identity[String],
        MoreExecutors.directExecutor
      )
    }

    /**
      * Construct a future which will either complete with the stringified
      * payload resulting from querying a url, or fail.
      *
      * @param client the HTTP client to use in the query
      * @param url the URL to query
      */
    private def get(client: OkHttpClient, url: String): ListenableFuture[String] = {
      val request = new Request.Builder()
        .url(url)
        .get
        .build

      val result = SettableFuture.create[String]()

      client
        .newCall(request)
        .enqueue(new Callback {
          override def onFailure(request: Request, e: IOException): Unit = {
            result.setException(e)
            ()
          }

          override def onResponse(response: Response): Unit = {
            if (response.isSuccessful) {
              result.set(response.body.string)
            } else if (response.code() == 404) {
              result.set("""{ "@graph": [] }""")
            } else {
              result.setException(
                new RuntimeException(s"ENCODE lookup failed: $response")
              )
            }
            ()
          }
        })

      result
    }

    override protected def newClient(): OkHttpClient = client
  }

  /**
    * Pipeline stage which maps batches of search parameters into JSON entities
    * from ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  def getEntities(
    encodeEntity: EncodeEntity
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Download ${encodeEntity.entryName} Entities") {
      _.applyKvTransform(ParDo.of(new EncodeLookup(encodeEntity))).flatMap { kv =>
        kv.getValue.asScala.fold(
          throw _,
          value => {
            val decoded = for {
              json <- io.circe.parser.parse(value)
              cursor = json.hcursor
              objects <- cursor.downField("@graph").as[Vector[JsonObject]]
            } yield {
              objects
            }
            decoded.fold(throw _, identity)
          }
        )
      }
    }

  /**
    * Pipeline stage which extracts IDs from downloaded JSON entities for
    * use in subsequent queries.
    *
    * @param entryName display name for the type of entity whose IDs will
    *                  be extracted in this stage
    * @param referenceField field in the input JSONs containing the IDs
    *                       to extract
    * @param manyReferences whether or not `referenceField` is an array
    */
  def getIds(
    entryName: String,
    referenceField: String,
    manyReferences: Boolean
  ): SCollection[JsonObject] => SCollection[String] =
    _.transform(s"Get $entryName IDs") { collection =>
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
    * Pipeline stage which maps entity IDs into corresponding JSON entities
    * downloaded from ENCODE.
    *
    * @param encodeEntity the type of ENCODE entity the input IDs correspond to
    */
  def getEntitiesById(
    encodeEntity: EncodeEntity
  ): SCollection[String] => SCollection[JsonObject] = { idStream =>
    val paramsBatchStream =
      idStream.transform(s"Build ${encodeEntity.entryName} ID Queries") {
        _.map(KV.of("key", _))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .applyKvTransform(GroupIntoBatches.ofSize(100))
          .map(_.getValue.asScala)
          .map {
            _.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
              ("@id" -> ref) :: acc
            }
          }
      }

    getEntities(encodeEntity)(paramsBatchStream)
  }
}
