package org.broadinstitute.monster.etl.v2f

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import org.broadinstitute.monster.etl.UpackMsgCoder
import org.scalatest.Matchers
import upack.{Msg, Obj, Str}

class V2FUtilsSpec extends PipelineSpec with Matchers {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  behavior of "V2FUtils"

  private val tsvMsgOriginal = List {
    Obj(
      Str("key1") -> Str("v11"),
      Str("key2") -> Str("v21"),
      Str("key3") -> Str("v31"),
      Str("key4") -> Str("v41"),
      Str("key5") -> Str("v51")
    )
    Obj(
      Str("key1") -> Str("v12"),
      Str("key2") -> Str("v22"),
      Str("key3") -> Str("v32"),
      Str("key4") -> Str("v42"),
      Str("key5") -> Str("v52")
    )
  }

  private val tsvMsgMissingValues = List {
    Obj(
      Str("key1") -> Str("v11"),
      Str("key2") -> Str("v21"),
      Str("key4") -> Str("v41"),
      Str("key5") -> Str("v51")
    )
    Obj(
      Str("key1") -> Str("v12"),
      Str("key2") -> Str("v22"),
      Str("key3") -> Str("v32"),
      Str("key5") -> Str("v52")
    )
  }

  private val tsvMsgDiffOrder = List {
    Obj(
      Str("key1") -> Str("v11"),
      Str("key5") -> Str("v51"),
      Str("key2") -> Str("v21"),
      Str("key4") -> Str("v41"),
      Str("key3") -> Str("v31")
    )
    Obj(
      Str("key1") -> Str("v12"),
      Str("key5") -> Str("v52"),
      Str("key2") -> Str("v22"),
      Str("key4") -> Str("v42"),
      Str("key3") -> Str("v32")
    )
  }

  private val tsvMsgDiffCols = List {
    Obj(
      Str("key10") -> Str("v101"),
      Str("key20") -> Str("v201"),
      Str("key30") -> Str("v301"),
      Str("key40") -> Str("v401"),
      Str("key50") -> Str("v501")
    )
    Obj(
      Str("key10") -> Str("v102"),
      Str("key20") -> Str("v202"),
      Str("key30") -> Str("v302"),
      Str("key40") -> Str("v402"),
      Str("key50") -> Str("v502")
    )
  }

  // getReadableFiles
  it should "get TSV files as ReadableFiles given a pattern match" in {
    val fileNames = List(
      "tsvTestFileDiffCols.txt",
      "tsvTestFileDiffOrder.txt",
      "tsvTestFileMissingValues.txt",
      "tsvTestFileOriginal.txt"
    )
    val (_, readableFiles) = runWithLocalOutput { sc =>
      {
        V2FUtils
          .getReadableFiles(
            "src/test/scala/org/broadinstitute/monster/etl/v2f/*.txt",
            sc
          )
          .map(_.getMetadata.resourceId.getFilename)
      }
    }
    readableFiles should contain allElementsOf fileNames
  }

  it should "return an empty SCollection if nothing matches the pattern" in {
    val (_, readableFiles) = runWithLocalOutput { sc =>
      {
        V2FUtils.getReadableFiles(
          "src/test/scala/org/broadinstitute/monster/etl/v2f/*.foo",
          sc
        )
      }
    }
    readableFiles shouldBe empty
  }

  it should "throw an exception if nothing matches a specific file's pattern" in {
    an[Exception] shouldBe thrownBy {
      runWithLocalOutput { sc =>
        {
          V2FUtils.getReadableFiles(
            "src/test/scala/org/broadinstitute/monster/etl/v2f/thisfiledoesnotexist.txt",
            sc
          )
        }
      }
    }
  }

  // tsvToMsg
  it should "convert each row of the TSV in an input stream to a Msg" in {

    val (_, readMessages) = runWithLocalOutput { sc =>
      {
        V2FUtils.tsvToMsg("testTableName")(
          V2FUtils.getReadableFiles(
            "src/test/scala/org/broadinstitute/monster/etl/v2f/tsvTestFileOriginal.txt",
            sc
          )
        )
      }
    }

    val out = readMessages.map(_._2)

    out should contain allElementsOf tsvMsgOriginal
  }

  it should "skip key-value pairs in a TSV where the value is an empty string" in {

    val (_, readMessages) = runWithLocalOutput { sc =>
      {
        V2FUtils.tsvToMsg("testTableName")(
          V2FUtils.getReadableFiles(
            "src/test/scala/org/broadinstitute/monster/etl/v2f/tsvTestFileMissingValues.txt",
            sc
          )
        )
      }
    }

    val out = readMessages.map(_._2)

    out should contain allElementsOf tsvMsgMissingValues
  }

  it should "convert multiple TSVs correctly even if the columns are different" in {
    val (_, readMessages) = runWithLocalOutput { sc =>
      {
        V2FUtils.tsvToMsg("testTableName")(
          V2FUtils.getReadableFiles(
            "src/test/scala/org/broadinstitute/monster/etl/v2f/*.txt",
            sc
          )
        )
      }
    }

    val out = readMessages.map(_._2)

    out should contain allElementsOf tsvMsgMissingValues
    out should contain allElementsOf tsvMsgOriginal
    out should contain allElementsOf tsvMsgDiffOrder
    out should contain allElementsOf tsvMsgDiffCols
  }

  // addAncestryID
  it should "add the ancestry ID from the ReadableFile paths as a field with a key -> value of 'ancestry' -> ID" in {
    val testData = Map(
      "gs://path/to/metaanalysis/ancestry-specific/phenotype/ancestry=ancestryID/file" -> Obj(
        Str("field1") -> Str("val1")
      )
    )

    val expectedOutputObj: (String, Msg) = (
      "gs://path/to/metaanalysis/ancestry-specific/phenotype/ancestry=ancestryID/file",
      Obj(
        Str("field1") -> Str("val1"),
        Str("ancestry") -> Str("ancestryID")
      )
    )

    runWithContext { sc =>
      val withAncestryID = V2FUtils.addAncestryID("tablename")(sc.parallelize(testData))
      withAncestryID should haveSize(1)
      withAncestryID should containSingleValue(expectedOutputObj)
    }
  }

  it should "throw an exception if it cannot find the ID in the path" in {
    val testData = Map(
      "gs://path/to/metaanalysis/ancestry-specific/phenotype/wrongfield=ancestryID/file" -> Obj(
        Str("field1") -> Str("val1")
      )
    )
    an[Exception] shouldBe thrownBy {
      runWithContext { sc =>
        V2FUtils.addAncestryID("tablename")(sc.parallelize(testData))
      }
    }
  }
}
