package org.broadinstitute.monster.etl.v2f

import better.files._
import com.spotify.scio.testing._
import io.circe.Json
import org.scalatest.BeforeAndAfterAll
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.Matchers

class V2FIntegrationSpec extends PipelineSpec with Matchers with BeforeAndAfterAll {

  behavior of "V2F ETL Pipeline"

  private val truthDir = "v2f" / "src" / "it" / "test-files" / "outputs"
  private val compareDir = "v2f" / "src" / "it" / "test-files" / "outputs-to-compare"
  private val inputDirString = "v2f/src/it/test-files/inputs/"
  private val compareDirString = "v2f/src/it/test-files/outputs-to-compare"

  // the directory structure and files are created by the first test, so all we need to do is delete them afterwards
  override def afterAll(): Unit = {
    val file = File.apply(compareDirString)
    file.delete()
    ()
  }

  it should "write test data without throwing an error" in {
    runWithRealContext(PipelineOptionsFactory.create()) { sc =>
      ExtractionPipeline.convertAndWrite(sc, inputDirString, compareDirString)
    }.waitUntilDone()
  }

  /**
    *
    * Helper method to parse the output files into a comparable format.
    *
    * @param directory The path to the directory where the files live.
    * @param filePattern The glob pattern of files to read.
    * @return One Set of Json that has every json object written to the output files.
    */
  private def createSetFromFiles(directory: File, filePattern: String): Set[Json] = {
    directory
      .glob(filePattern)
      .flatMap { _.lineIterator }
      .map { line =>
        val maybeParsed = io.circe.parser.parse(line)
        maybeParsed.fold(
          err => throw new Exception(s"Failed to parse input line as JSON: $line", err),
          identity
        )
      }.toSet
  }

  /**
    *
    * Helper method to call the parsing method on the truth-files and the files-to-test.
    *
    * @param filePattern The glob pattern of files to read.
    * @return A tuple of Set of Json, where the first one is the Set-to-test and the second one is the truth-Set.
    */
  private def compareTruthAndCompSets(filePattern: String, description: String): Unit = {
    it should description in {
      createSetFromFiles(compareDir, filePattern) shouldBe createSetFromFiles(
        truthDir,
        filePattern
      )
    }
  }

  private val filePatternsAndDescriptions = Set(
    ("frequency-analysis/*.json", "have written the correct frequency-analysis data"),
    (
      "meta-analysis/ancestry-specific/*.json",
      "have written the correct ancestry-specific data"
    ),
    ("meta-analysis/trans-ethnic/*.json", "have written the correct trans-ethnic data"),
    (
      "variant-effect/regulatory-feature-consequences/*.json",
      "have written the correct regulatory-feature-consequences data"
    ),
    (
      "variant-effect/transcript-consequences/*.json",
      "have written the correct transcript-consequences data"
    ),
    ("variants/*.json", "have written the correct variants data")
  )

  filePatternsAndDescriptions.foreach {
    case (filePattern, description) =>
      it should behave like compareTruthAndCompSets(filePattern, description)
  }

}
