package org.broadinstitute.monster.etl.v2f

import com.spotify.scio.coders.Coder
import org.scalatest.Matchers
import com.spotify.scio.testing._
import org.broadinstitute.monster.etl.UpackMsgCoder
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

  it should "convert each row of the TSV in an input stream to a Msg" in {

    val (_, readMessages) = runWithLocalOutput { sc =>
      {
        V2FUtils.tsvToMsg("testTableName")(
          V2FUtils.getReadableFiles(
            "/Users/rarshad/Projects/monster-etl/v2f/src/test/scala/org/broadinstitute/monster/etl/v2f/*.txt",
            sc
          )
        )
      }
    }

    val out = readMessages.map(_._2)

    out should contain allElementsOf tsvMsgOriginal
  }

//  it should "skip key-value pairs in a TSV where the value is an empty string" in {
//  }
}
