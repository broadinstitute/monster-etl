package org.broadinstitute.monster.etl.encode.extract.client

import java.util.concurrent.Executors

import cats.effect._
import cats.syntax.all._
import fs2.Stream
import io.circe.{Json, JsonObject}
import org.http4s.{Method, Query, Request, Status, Uri}
import org.http4s.client.{Client, UnexpectedStatus}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers.Location

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Client for pulling information out of ENCODE's REST API.
  *
  * @param client generic blaze/http4s client handling the actual request-sending/parsing logic
  * @see https://www.encodeproject.org/help/rest-api/
  */
class EncodeClient private (client: Client[IO]) {

  /**
    * Pull all metadata for a specific ENCODE entity type matching a set of criteria.
    *
    * @param entityType category to search within. Must be one of the schema titles listed at https://www.encodeproject.org/profiles/
    * @param searchParams key-value pairs to match entities against. If a key is provided multiple times, entities
    *                     matching any of the associated values will be returned
    */
  def search(
    entityType: String,
    searchParams: Seq[(String, String)]
  ): Stream[IO, JsonObject] = {

    val allParams = Seq("type" -> entityType, "limit" -> "all", "format" -> "json") ++ searchParams
    val searchUri = EncodeClient.EncodeUri
      .withPath("/search/")
      .copy(query = Query.fromPairs(allParams: _*))

    val request = client.expectOr[Json](Request[IO](uri = searchUri)) { failedResponse =>
      IO.pure(failedResponse.status).map { code =>
        if (code == Status.NotFound) {
          EncodeClient.NoResultsFound
        } else {
          UnexpectedStatus(code)
        }
      }
    }(org.http4s.circe.jsonDecoder)

    Stream
      .eval(request)
      .flatMap { res =>
        res.hcursor
          .downField("@graph")
          .as[Seq[JsonObject]]
          .fold(Stream.raiseError[IO], jss => Stream.emits(jss))
      }
      .recoverWith {
        case EncodeClient.NoResultsFound => Stream.empty
      }
  }

  /**
    * Query ENCODE for the AWS download URI backing the download endpoint for a file entity.
    *
    * @param downloadEndpoint value of an "href" field pulled from file entity metadata
    */
  def deriveDownloadUri(downloadEndpoint: String): IO[Uri] = {

    val request = Request[IO](
      method = Method.HEAD,
      uri = EncodeClient.EncodeUri.withPath(downloadEndpoint)
    )
    client.fetch(request) { response =>
      response.headers
        .get(Location)
        .fold[IO[Uri]](
          IO.raiseError(
            new IllegalStateException(
              s"HEAD of $downloadEndpoint returned no redirect URI"
            )
          )
        ) { redirectLocation =>
          // Redirects look like:
          //  https://download.encodeproject.org/https://encode-files.s3.amazonaws.com/2016/10/14/a0ef19e5-d9d6-4984-b29d-47a64abf4d0d/ENCFF398VEH.bam?key=value&key2=value2
          // Google needs the embedded S3 uri:   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          IO.fromEither(Uri.fromString(redirectLocation.uri.path.dropWhile(_ == '/')))
        }
    }
  }
}

object EncodeClient {

  /**
    * Max number of requests a client should send to ENCODE at a time.
    *
    * FIXME: This should really be something the client enforces internally.
    */
  val Parallelism: Int = Runtime.getRuntime.availableProcessors()

  val EncodeUri: Uri = Uri.unsafeFromString("https://www.encodeproject.org")

  def resource(implicit CS: ContextShift[IO], t: Timer[IO]): Resource[IO, EncodeClient] =
    for {
      ec <- schedulerContext
      blaze <- BlazeClientBuilder[IO](ec).resource
    } yield {
      val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(1.second, 5))
      val wrappedBlaze =
        Retry(retryPolicy)(Logger(logHeaders = true, logBody = false)(blaze))
      new EncodeClient(wrappedBlaze)
    }

  private def schedulerContext =
    Resource[IO, ExecutionContext](IO.delay {
      val executor = Executors.newFixedThreadPool(1)
      val ec = ExecutionContext.fromExecutor(executor)
      (ec, IO.delay(executor.shutdown()))
    })

  private object NoResultsFound extends Throwable
}
