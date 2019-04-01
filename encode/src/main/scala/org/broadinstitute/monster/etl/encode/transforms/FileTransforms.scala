package org.broadinstitute.monster.etl.encode.transforms

import io.circe.syntax._

/** Stream transformations run only on file entities during ENCODE ETL. */
object FileTransforms {

  /**
    * Extract QC values from the nested "notes" field for File entities.
    *
    * ENCODE also exposes QC as entities in themselves; we should investigate
    * downloading & processing those entities instead of relying on bespoke
    * nested JSON.
    */
  def extractFileQc: JsonPipe = _.transform("Extract File QC") {
    _.map { json =>
      import io.circe.parser.parse

      val maybePercentAligned = for {
        // Drill down to QC wrapper:
        encodedNotes <- json("notes").flatMap(_.asString)
        notesObject <- parse(encodedNotes).toOption.flatMap(_.asObject)
        qcObject <- notesObject("qc")
          .flatMap(_.asObject)
          .flatMap(_("qc"))
          .flatMap(_.asObject)
        // Extract out QC values we care about:
        alignedReads <- qcObject("mapped")
          .flatMap(_.as[Array[Long]].toOption)
          .flatMap(_.headOption)
        totalReads <- qcObject("in_total")
          .flatMap(_.as[Array[Long]].toOption)
          .flatMap(_.headOption)
      } yield {
        alignedReads.toDouble / totalReads
      }

      maybePercentAligned
        .fold(json)(pct => json.add("percent_aligned_reads", pct.asJson))
        .remove("notes")
    }
  }

  private val pairedEndJson = "paired-ended".asJson

  /** Convert ENCODE's string description of paired-ended-ness into a more usable boolean. */
  def markFileRunType: JsonPipe = _.transform("Mark File Run Type") {
    _.map { json =>
      json("paired_end").fold(json) { pairedStr =>
        json.add("paired_end", pairedStr.equals(pairedEndJson).asJson)
      }
    }
  }
}
