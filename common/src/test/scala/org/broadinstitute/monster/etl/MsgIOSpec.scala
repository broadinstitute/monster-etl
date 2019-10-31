package org.broadinstitute.monster.etl

import better.files.File
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import upack._

class MsgIOSpec extends PipelineSpec {
  behavior of "MsgIO"

  private implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  private val jsons = List.tabulate(100) { i =>
    s"""{"foo":"bar","baz":[$i,"nan",${i + 1}],"qux":{"even?":${i % 2 == 0}}}"""
  }

  private val msgs = List.tabulate[Msg](100) { i =>
    Obj(
      Str("foo") -> Str("bar"),
      Str("baz") -> Arr(
        Int32(i),
        Str("nan"),
        Int32(i + 1)
      ),
      Str("qux") -> Obj(
        Str("even?") -> Bool(i % 2 == 0)
      )
    )
  }

  it should "read messages as JSON-list on exact matches" in {
    File.temporaryDirectory().foreach { tmpDir =>
      val in = (tmpDir / "data.json").write(jsons.mkString("\n"))
      val (_, readMessages) = runWithLocalOutput { sc =>
        MsgIO.readJsonLists(sc, "Test Single", in.pathAsString)
      }
      readMessages should contain allElementsOf msgs
    }
  }

  it should "read messages as JSON-list on glob matches" in {
    File.temporaryDirectory().foreach { tmpDir =>
      jsons.grouped(10).zipWithIndex.foreach {
        case (lines, i) =>
          (tmpDir / s"part$i.json").write(lines.mkString("\n"))
          ()
      }
      val (_, readMessages) = runWithLocalOutput { sc =>
        MsgIO.readJsonLists(sc, "Test Multiple", s"${tmpDir.pathAsString}/*")
      }
      readMessages should contain allElementsOf msgs
    }
  }

  it should "write messages as JSON-list" in {
    File.temporaryDirectory().foreach { tmpDir =>
      runWithRealContext(PipelineOptionsFactory.create()) { sc =>
        MsgIO.writeJsonLists(
          sc.parallelize(msgs),
          "Test Write",
          tmpDir.pathAsString
        )
      }
      val written =
        tmpDir.listRecursively().filter(_.name.endsWith(".json")).flatMap(_.lines).toList
      written should contain allElementsOf jsons
    }
  }
}
